package mysql_binlog_utils

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func innerDumpBinlogFromPos(srcFilePath string, startPos uint, dumpEmptyBinlog bool, targetFilePath string) error {
	tracef("dump binlog from pos : srcFilePath=%v, startPos=%v, targetFilePath=%v", srcFilePath, startPos, targetFilePath)
	srcFile, err := os.Open(srcFilePath)
	if nil != err {
		return err
	}
	defer srcFile.Close()

	stat, err := srcFile.Stat()
	if nil != err {
		return err
	}
	srcFileSize := uint(stat.Size())

	if dumpEmptyBinlog {
		startPos = srcFileSize
	}

	if startPos > srcFileSize {
		return fmt.Errorf("startPos (%v) >= binlog file size (%v)", startPos, srcFileSize)
	}

	emptyFile := startPos == srcFileSize
	headerBs := make([]byte, 19)
	headerEndPos := uint(4)

	for {
		if _, err := srcFile.Seek(int64(headerEndPos), 0); nil != err {
			if "EOF" == err.Error() {
				break
			}
			return err
		}

		if _, err := io.ReadFull(srcFile, headerBs); nil != err {
			if "EOF" == err.Error() {
				break
			}
			return err
		}

		eventType := int(headerBs[4])
		length := binary.LittleEndian.Uint32(headerBs[9:13])
		if FORMAT_DESCRIPTION_EVENT != eventType && ROTATE_EVENT != eventType && PREVIOUS_GTIDS_LOG_EVENT != eventType {
			break
		}
		headerEndPos += uint(length)
	}

	if headerEndPos >= srcFileSize {
		emptyFile = true
	} else if startPos < headerEndPos {
		return fmt.Errorf("dump binlog from startPos (%v) failed, pos < headerEndPos (%v) ", startPos, headerEndPos)
	}

	if target, err := os.Create(targetFilePath); nil != err {
		return err
	} else {
		defer target.Close()
		if _, err := srcFile.Seek(0, 0); nil != err {
			os.Remove(targetFilePath)
			return err
		}

		if _, err := io.CopyN(target, srcFile, int64(headerEndPos)); nil != err {
			os.Remove(targetFilePath)
			return err
		}

		if !emptyFile {
			if _, err := srcFile.Seek(int64(startPos), 0); nil != err {
				os.Remove(targetFilePath)
				return err
			}

			if _, err := io.Copy(target, srcFile); nil != err {
				os.Remove(targetFilePath)
				return err
			}
		}
	}

	return nil
}

func DumpBinlogFromPos(srcFilePath string, startPos uint, targetFilePath string) error {
	return innerDumpBinlogFromPos(srcFilePath, startPos, false, targetFilePath)
}

func DumpUnexecutedBinlogByGtid(srcFilePath string, executedGtidDesc string, targetFilePath string, includeEventBeforeFirst bool) error {
	pos, err := GetUnexecutedBinlogPosByGtid(srcFilePath, executedGtidDesc, includeEventBeforeFirst)
	if nil != err {
		if "EOF" == err.Error() {
			return innerDumpBinlogFromPos(srcFilePath, 0, true, targetFilePath)
		} else {
			return err
		}
	}
	return innerDumpBinlogFromPos(srcFilePath, pos, false, targetFilePath)
}
