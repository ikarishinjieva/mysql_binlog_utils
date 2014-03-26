package mysql_binlog_utils

import ()

func GenBinlogEventBytes(fh EventFixedHeader, fd EventFixedData, vd EventVariableData) ([]byte, error) {
	eventLength := LOG_EVENT_FIXED_HEADER_LEN + len(fd.Bytes) + len(vd.Bytes)
	fh.EventLength = eventLength

	buf := make([]byte, eventLength)
	intToBytes(fh.Timestamp, buf[0:4])
	intToBytes(fh.EventType, buf[4:5])
	intToBytes(fh.ServerId, buf[5:9])
	intToBytes(fh.EventLength, buf[9:13])
	intToBytes(fh.NextPosition, buf[13:17])
	intToBytes(fh.Flags, buf[17:19])
	copy(buf[19:19+len(fd.Bytes)-1], fd.Bytes)
	copy(buf[19+len(fd.Bytes):], vd.Bytes)
	return buf, nil
}
