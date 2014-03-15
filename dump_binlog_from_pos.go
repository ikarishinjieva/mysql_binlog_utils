package mysql_binlog_util

import (
	"fmt"
	"io"
	"os"
)

func DumpBinlogFromPos(srcFilePath string, startPos int, targetFilePath string) error {
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

	if startPos >= parser.FileSize() {
		return fmt.Errorf("startPos (%v) >= binlog file size (%v)", startPos, parser.FileSize())
	}

	headerEndPos := 4
	for {
		if e, err := parser.ReadEventFixedHeader(headerEndPos); nil != err {
			return err
		} else if FORMAT_DESCRIPTION_EVENT != e.eventType && ROTATE_EVENT != e.eventType && PREVIOUS_GTIDS_LOG_EVENT != e.eventType {
			break
		} else {
			headerEndPos = e.nextPosition
		}
	}

	if startPos < headerEndPos {
		return fmt.Errorf("dump binlog from startPos (%v) failed, pos < headerEndPos (%v) ", startPos, headerEndPos)
	}

	if target, err := os.Create(targetFilePath); nil != err {
		return err
	} else {
		if _, err := srcFile.Seek(0, 0); nil != err {
			target.Close()
			os.Remove(targetFilePath)
			return err
		}

		if _, err := io.CopyN(target, srcFile, int64(headerEndPos)); nil != err {
			target.Close()
			os.Remove(targetFilePath)
			return err
		}

		if _, err := srcFile.Seek(int64(startPos), 0); nil != err {
			target.Close()
			os.Remove(targetFilePath)
			return err
		}

		if _, err := io.Copy(target, srcFile); nil != err {
			target.Close()
			os.Remove(targetFilePath)
			return err
		}
		defer target.Close()
	}

	return nil
}
