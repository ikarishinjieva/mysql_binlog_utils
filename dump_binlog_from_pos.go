package mysql_binlog_utils

import (
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

	parser, err := NewBinlogFileParser(srcFile)
	if nil != err {
		return err
	}
	defer parser.Destroy()

	if dumpEmptyBinlog {
		startPos = parser.FileSize()
	}

	if startPos > parser.FileSize() {
		return fmt.Errorf("startPos (%v) >= binlog file size (%v)", startPos, parser.FileSize())
	}

	emptyFile := startPos == parser.FileSize()

	headerEndPos := uint(4)
	for {
		if headerEndPos >= parser.FileSize() {
			break
		}
		if e, err := parser.ReadEventFixedHeader(headerEndPos); nil != err {
			return err
		} else if FORMAT_DESCRIPTION_EVENT != e.EventType && ROTATE_EVENT != e.EventType && PREVIOUS_GTIDS_LOG_EVENT != e.EventType {
			break
		} else {
			headerEndPos = headerEndPos + e.EventLength
		}
	}

	if headerEndPos >= parser.FileSize() {
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

func DumpUnexecutedBinlogByGtid(srcFilePath string, executedGtidDesc string, targetFilePath string) error {
	pos, err := GetUnexecutedBinlogPosByGtid(srcFilePath, executedGtidDesc)
	if nil != err && "EOF" == err.Error() {
		if "EOF" == err.Error() {
			return innerDumpBinlogFromPos(srcFilePath, 0, true, targetFilePath)
		} else {
			return err
		}
	}
	return innerDumpBinlogFromPos(srcFilePath, pos, false, targetFilePath)
}
