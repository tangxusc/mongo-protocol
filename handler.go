package mongo_protocol

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net"
)

var defaultHandler = &PrintHandler{}

type Handler interface {
	Process(header *MsgHeader, r *Reader, conn *ConnContext) error
}

type PrintHandler struct {
}

func (d *PrintHandler) Process(header *MsgHeader, r *Reader, conn *ConnContext) error {
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
	logrus.Infof(`[server]PrintHandler message: %s`, bytes)
	reply := NewReply(header.RequestID)
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{"ok": 1}
	e = reply.Write(conn)
	return nil
}

type QueryHandler interface {
	Support(query *Query) bool
	Process(query *Query, reply *Reply) error
}

type ConnContext struct {
	net.Conn
	m map[string]interface{}
}

func (c *ConnContext) Set(key string, value interface{}) {
	c.m[key] = value
}
func (c *ConnContext) Get(key string) (value interface{}, ok bool) {
	value, ok = c.m[key]
	return
}
func NewConnContext(conn net.Conn) *ConnContext {
	return &ConnContext{
		Conn: conn,
		m:    make(map[string]interface{}),
	}
}
