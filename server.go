package mongo_protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
)

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
	connContext := NewConnContext(conn)
	defer connContext.Close()
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
			e := binary.Read(connContext, binary.LittleEndian, header)
			if e != nil {
				if e != io.EOF {
					logrus.Errorf(`[server]unexpected error:%v on port [%s]`, e, server.Port)
				}
				break
			}
			rd := io.LimitReader(connContext, int64(header.MessageLength-4*4))
			r := &Reader{rd}
			server.process(header, connContext, e, r)
		}
	}
}

func (server *Server) process(header *MsgHeader, connContext *ConnContext, e error, r *Reader) {
	defer func() {
		if e := recover(); e != nil {
			writeError(header, e, connContext)
		}
	}()
	h, ok := server.handlerMap[header.OpCode]
	if !ok {
		e = server.defaultHandler.Process(header, r, connContext)
	} else {
		e = h.Process(header, r, connContext)
	}
	if e != nil {
		logrus.Errorf(`[server]process error:%v on port [%s]`, e, server.Port)
		writeError(header, e, connContext)
	}
}

func writeError(header *MsgHeader, e interface{}, connContext *ConnContext) {
	reply := NewReply(header.RequestID)
	reply.ResponseFlags = QueryFailure
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{"$err": fmt.Sprintf("%v", e)}
	if e := reply.Write(connContext); e != nil {
		panic(e)
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
