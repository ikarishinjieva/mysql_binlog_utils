package mysql_binlog_utils

import (
	"fmt"
	"strconv"
	"strings"
)

type tGtid struct {
	sids []tSid
}

type tSid struct {
	serverUuid string
	intervals  []tInterval
}

type tInterval struct {
	from uint64
	to   uint64
}

func (g *tGtid) String() (ret string) {
	for _, sid := range g.sids {
		s := sid.serverUuid
		for _, interval := range sid.intervals {
			s = fmt.Sprintf("%v:%v-%v", s, interval.from, interval.to)
		}
		if "" != ret {
			ret = ret + ","
		}
		ret = ret + s
	}
	return ret
}

func newSingleGtid(uuid string, num uint64) tGtid {
	gtid := tGtid{}
	sid := tSid{}
	sid.serverUuid = uuid
	interval := tInterval{num, num}
	sid.intervals = []tInterval{interval}
	gtid.sids = []tSid{sid}
	return gtid
}

func parseGtid(desc string) (gtid tGtid, err error) {
	desc = strings.TrimSpace(desc)
	if "" == desc {
		return gtid, nil
	}
	for _, sidNumber := range strings.Split(desc, ",") {
		sidNumber = strings.TrimSpace(sidNumber)
		if a := strings.Split(sidNumber, ":"); len(a) < 2 {
			return gtid, fmt.Errorf("invalid gtid %v", sidNumber)
		} else {
			sid := tSid{}
			sid.serverUuid = strings.ToUpper(strings.Replace(a[0], "-", "", -1))
			for i := 1; i < len(a); i++ {
				interval := tInterval{}
				seg := a[i]
				if splitPos := strings.Index(seg, "-"); -1 != splitPos {
					firstPart := string(seg[0:splitPos])
					if i64, err := strconv.ParseUint(firstPart, 10, 64); nil == err {
						interval.from = i64
					} else {
						return gtid, fmt.Errorf("invalid number %v", firstPart)
					}
					secondPart := string(seg[splitPos+1:])
					if i64, err := strconv.ParseUint(secondPart, 10, 64); nil == err {
						interval.to = i64
					} else {
						return gtid, fmt.Errorf("invalid number %v", secondPart)
					}
				} else if i64, err := strconv.ParseUint(seg, 10, 64); nil == err {
					interval.from = i64
					interval.to = i64
				} else {
					return gtid, fmt.Errorf("invalid number %v", seg)
				}
				sid.intervals = append(sid.intervals, interval)
			}
			gtid.sids = append(gtid.sids, sid)
		}
	}
	return gtid, nil
}

func containsGtid(current tGtid, reference tGtid) bool {
	for _, rSid := range reference.sids {
		//find match sid
		found := false
		var sid tSid
		for _, a := range current.sids {
			if rSid.serverUuid == a.serverUuid {
				found = true
				sid = a
				break
			}
		}
		if !found {
			return false
		}

		//if interval contains
		for _, rInterval := range rSid.intervals {
			found := false
			for _, a := range sid.intervals {
				if a.from <= rInterval.from && a.to >= rInterval.to {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func getPreviousGtids(binlogPath string) (gtid tGtid, err error) {
	parser, err := NewBinlogFileParserByPath(binlogPath)
	if nil != err {
		return gtid, err
	}
	defer parser.Destroy()
	p := uint(4)
	for {
		header, bs, err := parser.ReadEventBytes(p)
		if nil != err {
			return gtid, err
		}
		if PREVIOUS_GTIDS_LOG_EVENT != header.EventType {
			p = p + header.EventLength
			continue
		}
		payload := bs[19:]
		sidNumberCount := bytesToUint(payload[0:8])
		pos := 8
		for i := uint(0); i < sidNumberCount; i++ {
			sidNumber := tSid{}
			sidNumber.serverUuid = bytesToUuid(payload[pos : pos+16])
			internalCount := bytesToUint(payload[pos+16 : pos+16+8])
			pos = pos + 16 + 8

			for i := uint(0); i < internalCount; i++ {
				internal := tInterval{}
				internal.from = bytesToUint64(payload[pos : pos+8])
				internal.to = bytesToUint64(payload[pos+8:pos+16]) - 1
				pos = pos + 16
				sidNumber.intervals = append(sidNumber.intervals, internal)
			}
			gtid.sids = append(gtid.sids, sidNumber)
		}
		return gtid, nil
	}
}
