package mongo_protocol

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"net"
)

var defaultHandler = &PrintHandler{}

type Reader struct {
	io.Reader
}

func (r *Reader) ReadInt32() (n int32, err error) {
	err = binary.Read(r, binary.LittleEndian, &n)
	return
}

func (r *Reader) ReadInt64() (*int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		if err == io.EOF {
			return &n, err
		}
		return &n, err
	}
	return &n, nil
}

func (r *Reader) ReadBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := r.Read(b)
	if err != nil {
		if err == io.EOF {
			return b, err
		}
		return b, err
	}
	return b, err
}

func (r *Reader) ReadCString() (string, error) {
	var b []byte
	var one = make([]byte, 1)
	for {
		_, err := r.Read(one)
		if err != nil {
			return "", err
		}
		if one[0] == '\x00' {
			break
		}
		b = append(b, one[0])
	}
	return string(b), nil
}

func (r *Reader) ReadOne() ([]byte, error) {
	docLen, err := r.ReadInt32()
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, err
	}
	buf := make([]byte, int(docLen))
	binary.LittleEndian.PutUint32(buf, uint32(docLen))
	if _, err := io.ReadFull(r, buf[4:]); err != nil {
		return buf, err
	}
	return buf, nil
}

func (r *Reader) ReadDocument() (m bson.M, e error) {
	bytes, e := r.ReadOne()
	if e != nil && e != io.EOF {
		return
	}
	if bytes != nil {
		e = bson.Unmarshal(bytes, &m)
	}

	return
}

func (r *Reader) ReadDocuments() (ms []bson.M, e error) {
	ms = make([]bson.M, 0)
	for {
		m, e := r.ReadDocument()
		if e != nil && e != io.EOF {
			break
		}
		if m == nil {
			break
		}
		ms = append(ms, m)
		if e == io.EOF {
			break
		}
	}
	return
}

type Handler interface {
	Process(header *MsgHeader, r *Reader, w io.Writer) error
}

type PrintHandler struct {
}

func (d *PrintHandler) Process(header *MsgHeader, r *Reader, w io.Writer) error {
	logrus.Infof(`header:%v`, *header)
	var data interface{}
	var e error
	switch header.OpCode {
	case OP_QUERY:
		query := &Query{}
		e = query.UnMarshal(r)
		data = query
	case OP_INSERT:
		insert := &Insert{}
		e = insert.UnMarshal(r)
		data = insert
	case OP_DELETE:
		i := &Delete{}
		e = i.UnMarshal(r)
		data = i
	case OP_UPDATE:
		i := &Update{}
		e = i.UnMarshal(r)
		data = i
	case OP_MSG:
		i := &Msg{}
		e = i.UnMarshal(r)
		data = i
	case OP_GET_MORE:
		i := &GetMore{}
		e = i.UnMarshal(r)
		data = i
	case OP_KILL_CURSORS:
		i := &KillCursors{}
		e = i.UnMarshal(r)
		data = i
	default:
		logrus.Warningf("[server]Unsupported messages for header.OpCode=[%v]", header.OpCode)
		_, e = ioutil.ReadAll(r)
	}
	if e != nil {
		return e
	}
	bytes, _ := json.Marshal(data)
	logrus.Infof(`[server]handler message: %s`, bytes)
	return nil
}

type Server struct {
	Port           string
	handlerMap     map[OpCode]Handler
	defaultHandler Handler
}

func (server *Server) Start(ctx context.Context) error {
	listener, e := net.Listen(`tcp`, fmt.Sprintf(`:%s`, server.Port))
	if e != nil {
		return e
	}
	for {
		select {
		case <-ctx.Done():
			return listener.Close()
		default:
			conn, e := listener.Accept()
			if e != nil {
				logrus.Errorf(`[server]accept port [%s] connection error:%v`, server.Port, e)
				continue
			}
			go server.handler(ctx, conn)
		}
	}
}

func (server *Server) handler(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	defer func() {
		if e := recover(); e != nil {
			logrus.Errorf(`[server]process error:%v on port [%s]`, e, server.Port)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			header := &MsgHeader{}
			e := binary.Read(conn, binary.LittleEndian, header)
			if e != nil {
				if e != io.EOF {
					logrus.Errorf(`[server]unexpected error:%v on port [%s]`, e, server.Port)
				}
				break
			}
			rd := io.LimitReader(conn, int64(header.MessageLength-4*4))
			r := &Reader{rd}
			h, ok := server.handlerMap[header.OpCode]
			if !ok {
				e = server.defaultHandler.Process(header, r, conn)
			} else {
				e = h.Process(header, r, conn)
			}
			if e != nil {
				logrus.Errorf(`[server]process error:%v on port [%s]`, e, server.Port)
			}
		}
	}
}

func (server *Server) AddHandler(code OpCode, handler Handler) {
	server.handlerMap[code] = handler
}

func (server *Server) SetDefaultHandler(handler Handler) {
	server.defaultHandler = handler
}

func NewServer(port string) *Server {
	return &Server{
		Port:           port,
		handlerMap:     make(map[OpCode]Handler),
		defaultHandler: defaultHandler,
	}
}
