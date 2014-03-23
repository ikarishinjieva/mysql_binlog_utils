package mysql_binlog_util

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//Packet

const (
	MAX_PACKET_LENGTH = 1<<24 - 1
)

type Packet struct {
	Payload  []byte
	Sequence int
}

func (p *Packet) AddInt1(a int) {
	buf := make([]byte, 1)
	p.Payload = append(p.Payload, intToBytes(a, buf)...)
}

func (p *Packet) AddInt2(a int) {
	buf := make([]byte, 2)
	p.Payload = append(p.Payload, intToBytes(a, buf)...)
}

func (p *Packet) AddInt4(a int) {
	buf := make([]byte, 4)
	p.Payload = append(p.Payload, intToBytes(a, buf)...)
}

func (p *Packet) AddStringNull(a string) {
	p.Payload = append(p.Payload, stringNullToBytes(a)...)
}

func (p *Packet) AddBytes(a []byte) {
	p.Payload = append(p.Payload, a...)
}

func (p *Packet) AddLengthEncodedInteger(a int) {
	if a < 251 {
		p.Payload = append(p.Payload, byte(a))
	} else {
		panic("not supported yet")
	}
}

func (p *Packet) AddLengthEncodedString(a string) {
	p.AddLengthEncodedInteger(len(a))
	p.Payload = append(p.Payload, []byte(a)...)
}

func (p *Packet) ToNetBytes() []byte {
	payloadLength := len(p.Payload)
	ret := make([]byte, payloadLength+4)
	intToBytes(payloadLength, ret[0:3])
	ret[3] = byte(p.Sequence)

	copy(ret[4:], p.Payload[0:])
	return ret
}

func (p *Packet) IsErrPacket() bool {
	return 0xff == p.Payload[0]
}

func NewPacket(seq int) *Packet {
	p := Packet{}
	p.Sequence = seq
	return &p
}

//FakeMasterServer

type FakeMasterServer struct {
	port                int
	keepAliveWhenFinish bool
	baseDir             string
	conn                *net.Conn
	abortChan           chan bool
	unusedServerId      int
	characterSet        int //SELECT id, collation_name FROM information_schema.collations ORDER BY id, utf8_general_ci=33
}

func NewFakeMasterServer(port int, unusedServerId int, characterSet int, keepAliveWhenFinish bool, baseDir string) *FakeMasterServer {
	server := FakeMasterServer{}
	server.port = port
	server.unusedServerId = unusedServerId
	server.characterSet = characterSet
	server.keepAliveWhenFinish = keepAliveWhenFinish
	server.baseDir = strings.Replace(baseDir, "\\", "/", -1)
	server.abortChan = make(chan bool, 0)
	return &server
}

func (f *FakeMasterServer) makeHandshakeV10Packet(sequence int) *Packet {
	auth_random_string := []byte("_fake_master_server_")

	packet := NewPacket(sequence)
	packet.AddInt1(10)                                   //protocal version
	packet.AddStringNull("5.5.33_fake_server")           //server version
	packet.AddInt4(1)                                    //connection id
	packet.AddBytes(auth_random_string[0:8])             //auth_plugin_data_part_1
	packet.AddInt1(0)                                    //filler
	packet.AddInt2(0)                                    //capability_flags_low
	packet.AddInt1(f.characterSet)                       //character_set
	packet.AddInt2(0)                                    //capability_flags_upper
	packet.AddInt1(21)                                   //auth_plugin_data_len
	packet.AddBytes(make([]byte, 10))                    //reserved
	packet.AddStringNull(string(auth_random_string[8:])) //auth_plugin_data_part_2
	return packet
}

func (f *FakeMasterServer) makeOkPacket(sequence int) *Packet {
	packet := NewPacket(sequence)
	packet.AddInt1(0) //header
	packet.AddInt1(0) //affected_rows
	packet.AddInt1(0) //last_insert_id
	packet.AddInt2(0) //status flags //AUTOCOMMIT?
	return packet
}

func (f *FakeMasterServer) makeQueryResponse(sequence int, columnCount int) *Packet {
	packet := NewPacket(sequence)
	packet.AddLengthEncodedInteger(columnCount)
	return packet
}

func (f *FakeMasterServer) makeEofPacket(sequence int) *Packet {
	packet := NewPacket(sequence)
	packet.AddInt1(0xfe) //the EOF header
	packet.AddInt2(0)    //warning count
	packet.AddInt2(0)    //status flags
	return packet
}

func (f *FakeMasterServer) makeUnixTimestampColumnDefPacket(sequence int) *Packet {
	packet := NewPacket(sequence)
	packet.AddLengthEncodedString("def")              //catalog
	packet.AddLengthEncodedString("")                 //schema
	packet.AddLengthEncodedString("")                 //table
	packet.AddLengthEncodedString("")                 //org_table
	packet.AddLengthEncodedString("UNIX_TIMESTAMP()") //name
	packet.AddLengthEncodedString("")                 //org_name
	packet.AddLengthEncodedInteger(0x0c)              //length of fixed-length fields [0c]
	packet.AddInt2(f.characterSet)                    //character_set
	packet.AddInt4(10)                                //column length
	packet.AddInt1(8)                                 //type=MYSQL_TYPE_LONGLONG
	packet.AddInt2(0)                                 //flags
	packet.AddInt1(0)                                 //decimals
	packet.AddInt2(0)                                 //filler
	return packet
}

func (f *FakeMasterServer) makeUnixTimestampRowPacket(sequence int) *Packet {
	packet := NewPacket(sequence)
	packet.AddLengthEncodedString(fmt.Sprintf("%v", time.Now().Unix()))
	return packet
}

func (f *FakeMasterServer) makeShowServerIdColumn0DefPacket(sequence int) *Packet {
	packet := NewPacket(sequence)
	packet.AddLengthEncodedString("def")                //catalog
	packet.AddLengthEncodedString("information_schema") //schema
	packet.AddLengthEncodedString("VARIABLES")          //table
	packet.AddLengthEncodedString("VARIABLES")          //org_table
	packet.AddLengthEncodedString("Variable_name")      //name
	packet.AddLengthEncodedString("VARIABLE_NAME")      //org_name
	packet.AddLengthEncodedInteger(0x0c)                //length of fixed-length fields [0c]
	packet.AddInt2(f.characterSet)                      //character_set
	packet.AddInt4(192)                                 //column length
	packet.AddInt1(15)                                  //type=MYSQL_TYPE_VARCHAR
	packet.AddInt2(0)                                   //flags
	packet.AddInt1(0)                                   //decimals
	packet.AddInt2(0)                                   //filler
	return packet
}

func (f *FakeMasterServer) makeShowServerIdColumn1DefPacket(sequence int) *Packet {
	packet := NewPacket(sequence)
	packet.AddLengthEncodedString("def")                //catalog
	packet.AddLengthEncodedString("information_schema") //schema
	packet.AddLengthEncodedString("VARIABLES")          //table
	packet.AddLengthEncodedString("VARIABLES")          //org_table
	packet.AddLengthEncodedString("Value")              //name
	packet.AddLengthEncodedString("VARIABLE_VALUE")     //org_name
	packet.AddLengthEncodedInteger(0x0c)                //length of fixed-length fields [0c]
	packet.AddInt2(f.characterSet)                      //character_set
	packet.AddInt4(3072)                                //column length
	packet.AddInt1(15)                                  //type=MYSQL_TYPE_VARCHAR
	packet.AddInt2(0)                                   //flags
	packet.AddInt1(0)                                   //decimals
	packet.AddInt2(0)                                   //filler
	return packet
}

func (f *FakeMasterServer) makeShowServerIdRowPacket(sequence int) *Packet {
	packet := NewPacket(sequence)
	packet.AddLengthEncodedString("server_id")
	packet.AddLengthEncodedString(fmt.Sprintf("%v", f.unusedServerId))
	return packet
}

func (f *FakeMasterServer) makeBinlogEventPacket(sequence int, event []byte) (ret []*Packet, nextSequence int) {
	start := 0
	length := len(event)
	firstPacket := true
	for start < length {
		packet := NewPacket(sequence)
		leftMaxLength := MAX_PACKET_LENGTH
		if firstPacket {
			packet.AddInt1(0)
			leftMaxLength--
			firstPacket = false
		}
		if start+leftMaxLength < length {
			packet.AddBytes(event[start : start+leftMaxLength])
		} else {
			packet.AddBytes(event[start:])
		}
		ret = append(ret, packet)
		sequence += 1
		start += leftMaxLength
	}

	return ret, sequence
}

func (f *FakeMasterServer) readPacket(conn net.Conn) (packet *Packet, err error) {
	readChan := make(chan bool, 1)
	packet = &Packet{}

	go func() {
		header := make([]byte, 4)
		if _, err = conn.Read(header); nil != err {
			tracef("read packet header got err=%v", err)
			readChan <- false
			return
		} else {
			length := bytesToUint(header[0:3])
			packet.Sequence = int(header[3])
			packet.Payload = make([]byte, length)
			if _, err = conn.Read(packet.Payload); nil != err {
				tracef("read packet payload got err=%v", err)
				readChan <- false
				return
			} else {
				readChan <- true
				return
			}
		}
	}()

	select {
	case <-readChan:
		return packet, err
	case <-f.abortChan:
		return nil, fmt.Errorf("abort")
	}
}

func (f *FakeMasterServer) Start() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("0.0.0.0:%v", f.port))
	if nil != err {
		return err
	}

	listener, err := net.ListenTCP("tcp4", tcpAddr)
	if nil != err {
		return err
	}

	conn, err := listener.Accept()
	if nil != err {
		return err
	}

	tracef("got a conn")

	f.conn = &conn

	defer conn.Close()

	if err := f.handshakePhase(conn); nil != err {
		return err
	}

	if err := f.communicatePhase(conn); nil != err {
		return err
	}

	return nil
}

func (f *FakeMasterServer) Abort() error {
	if nil != f.conn {
		(*f.conn).Close()
	}
	f.abortChan <- true
	return nil
}

func (f *FakeMasterServer) sendPacket(name string, conn net.Conn, packet *Packet) error {
	tracef("send packet %v, payload_len=%v", name, len(packet.Payload))
	if _, err := conn.Write(packet.ToNetBytes()); nil != err {
		tracef("send packet %v got err=%v, packet=%v", name, err, packet)
		return err
	}
	return nil
}

func (f *FakeMasterServer) communicatePhase(conn net.Conn) error {
	for {
		packet, err := f.readPacket(conn)
		if nil != err {
			return err
		}
		switch packet.Payload[0] {
		case 0x3:
			tracef("read query packet %v", string(packet.Payload[1:]))
			err = f.handleQueryPacket(packet, conn)
		case 0x15:
			tracef("read COM_REGISTER_SLAVE packet")
			err = f.handleComRegisterSlavePacket(packet, conn)
		case 0x12:
			tracef("read COM_BINLOG_DUMP packet")
			return f.handleComBinlogDump(packet, conn)
		default:
			err = fmt.Errorf("unsupported packet %x", packet.Payload[0])
		}
		if nil != err {
			return err
		}
	}
}

func (f *FakeMasterServer) handleComBinlogDump(packet *Packet, conn net.Conn) error {
	pos := bytesToUint(packet.Payload[1:5])
	path := filepath.Join(f.baseDir, string(packet.Payload[11:]))
	seq := 1

	for {
		tracef("slave request binlog dump from %v:%v", path, pos)
		parser, err := NewBinlogFileParserByPath(path)
		if nil != err {
			return err
		}
		defer parser.Destroy()

		if 0 == pos {
			pos = 4
		}

		//transfer binlog header first
		if 4 != pos {
			p := 4
			for {
				if header, bs, err := parser.ReadEventBytes(p); nil != err {
					return err
				} else if FORMAT_DESCRIPTION_EVENT != header.EventType && ROTATE_EVENT != header.EventType && PREVIOUS_GTIDS_LOG_EVENT != header.EventType {
					break
				} else {
					packets, nextSeq := f.makeBinlogEventPacket(seq, bs)
					for _, packet := range packets {
						if err := f.sendPacket(fmt.Sprintf("event @%v", p), conn, packet); nil != err {
							return err
						}
					}

					seq = nextSeq
					p = p + header.EventLength
				}
			}
		}

		//transfer binlog from pos
		for {
			select {
			case <-f.abortChan:
				return fmt.Errorf("abort")
			default:
			}
			header, bs, err := parser.ReadEventBytes(pos)
			if nil != err {
				if "EOF" == err.Error() {
					if nextBinlogPath, err := NextBinlogPath(path); nil != err {
						return err
					} else if _, err := os.Stat(nextBinlogPath); nil != err && os.IsNotExist(err) {
						if f.keepAliveWhenFinish {
							time.Sleep(100 * time.Millisecond)
							continue
						} else {
							//finish
							return nil
						}
					} else if nil != err {
						return err
					} else {
						parser.Destroy()
						path = nextBinlogPath
						pos = 4
						break
					}
				} else {
					return err
				}
			}
			packets, nextSeq := f.makeBinlogEventPacket(seq, bs)
			for _, packet := range packets {
				if err := f.sendPacket(fmt.Sprintf("event @%v", pos), conn, packet); nil != err {
					return err
				}
			}
			seq = nextSeq
			pos = pos + header.EventLength
		}
	}

	return nil
}

func (f *FakeMasterServer) handleComRegisterSlavePacket(packet *Packet, conn net.Conn) error {
	return f.sendPacket("COM_REGISTER_SLAVE ok response", conn, f.makeOkPacket(1))
}

func (f *FakeMasterServer) handleQueryPacket(packet *Packet, conn net.Conn) error {
	query := string(packet.Payload[1:])

	if "SELECT UNIX_TIMESTAMP()" == query {
		if err := f.sendPacket("query response", conn, f.makeQueryResponse(1, 1)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : column def", conn, f.makeUnixTimestampColumnDefPacket(2)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : eof", conn, f.makeEofPacket(3)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : row", conn, f.makeUnixTimestampRowPacket(4)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : eof", conn, f.makeEofPacket(5)); nil != err {
			return err
		}
		return nil
	}

	if "SHOW VARIABLES LIKE 'SERVER_ID'" == query {
		if err := f.sendPacket("query response", conn, f.makeQueryResponse(1, 2)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : column 0 def", conn, f.makeShowServerIdColumn0DefPacket(2)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : column 1 def", conn, f.makeShowServerIdColumn1DefPacket(3)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : eof", conn, f.makeEofPacket(4)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : row", conn, f.makeShowServerIdRowPacket(5)); nil != err {
			return err
		}
		if err := f.sendPacket("query response : eof", conn, f.makeEofPacket(6)); nil != err {
			return err
		}
		return nil
	}

	if strings.HasPrefix(query, "SET @master_heartbeat_period") {
		if err := f.sendPacket("query response : ok", conn, f.makeQueryResponse(1, 0)); nil != err {
			return err
		}
		return nil
	}

	return fmt.Errorf("unsupported query %v", query)
}

func (f *FakeMasterServer) handshakePhase(conn net.Conn) error {
	//send handshake
	if err := f.sendPacket("handshake packet", conn, f.makeHandshakeV10Packet(0)); nil != err {
		return err
	}

	//handshake response
	if packet, err := f.readPacket(conn); nil != err {
		return err
	} else if packet.IsErrPacket() {
		return fmt.Errorf("handshake got response err packet")
	}

	//send ok
	if err := f.sendPacket("ok packet", conn, f.makeOkPacket(2)); nil != err {
		return err
	}

	return nil
}
