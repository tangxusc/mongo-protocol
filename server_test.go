package mongo_protocol

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestNewServer(t *testing.T) {
	server := NewServer(`27017`)
	server.AddHandler(OP_QUERY, &TestHandler{})
	e := server.Start(context.TODO())
	if e != nil {
		panic(e)
	}
}

func TestMarshal(t *testing.T) {
	r, w, _ := os.Pipe()
	e := binary.Write(w, binary.LittleEndian, int64(100))
	if e != nil {
		panic(e)
	}
	w.Close()
	bytes, e := ioutil.ReadAll(r)
	fmt.Println(len(bytes), e)
}

type TestHandler struct {
}

func (t *TestHandler) Process(header *MsgHeader, r *Reader, w io.Writer) error {
	query := &Query{}
	e := query.UnMarshal(r)
	if e != nil {
		return e
	}
	bytes, _ := json.Marshal(query)
	fmt.Println(string(bytes))
	logrus.Debugf(`%s`, bytes)

	reply := NewReply(query.Header.RequestID)
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{"isMaster": 1, "ok": 1}

	e = reply.Write(w)
	if e != nil {
		return e
	}

	return nil
}
