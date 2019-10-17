package mongo_protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestNewServer(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetReportCaller(true)
	server := NewServer(`27018`)
	server.AddHandler(OP_QUERY, &TestHandler{})
	server.AddHandler(OP_MSG, &MsgHandler{})
	e := server.Start(context.TODO())
	if e != nil {
		panic(e)
	}
}

type MsgHandler struct {
}

func (m *MsgHandler) Process(header *MsgHeader, r *Reader, conn *ConnContext) error {
	msg := &Msg{}
	if e := msg.UnMarshal(r); e != nil {
		return e
	}
	marshal, _ := json.Marshal(msg)
	logrus.Debugf("test=-----------------------:%s", marshal)

	body := msg.GetBodyMsgSection()
	//insert
	_, ok := body["insert"]
	if ok {
		logrus.Debugf("insert------------------")
		msgReply := NewMsgReply(header.RequestID)
		section := NewBodyMsgSection()
		section.Body = bson.M{
			"n":  1,
			"ok": 1,
		}

		msgReply.Sections = append(msgReply.Sections, section)
		e := msgReply.Write(conn)
		return e
	}
	//find
	_, ok = body["find"]
	if ok {
		logrus.Debugf("find------------------")
		msgReply := NewMsgReply(header.RequestID)
		section := NewBodyMsgSection()
		section.Body = bson.M{
			"cursor": bson.M{
				"firstBatch": nil,
				//"id": bson.M{
				//	"$numberLong": "0",
				//},
				"id": int64(11),
				"ns": "aggregate.a1_event",
			},
			"ok": 1,
		}

		msgReply.Sections = append(msgReply.Sections, section)
		e := msgReply.Write(conn)
		return e
	}
	//isMaster
	_, ok = body["isMaster"]
	if ok {
		logrus.Debugf("isMaster------------------")
		msgReply := NewMsgReply(header.RequestID)
		section := NewBodyMsgSection()
		section.Body = bson.M{
			"ismaster":                     true,
			"maxBsonObjectSize":            16777216,
			"maxMessageSizeBytes":          48000000,
			"maxWriteBatchSize":            100000,
			"localTime":                    time.Now(),
			"logicalSessionTimeoutMinutes": 30,
			"connectionId":                 808,
			"minWireVersion":               0,
			"maxWireVersion":               3,
			"readOnly":                     false,
			"ok":                           1.0,
		}

		msgReply.Sections = append(msgReply.Sections, section)
		e := msgReply.Write(conn)
		return e
	}
	//

	return nil
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

func (t *TestHandler) Process(header *MsgHeader, r *Reader, conn *ConnContext) error {
	query := &Query{}
	e := query.UnMarshal(r)
	if e != nil {
		return e
	}
	bytes, _ := json.Marshal(query)
	//fmt.Println(string(bytes))
	logrus.Debugf(`TestHandler-----------------------%s`, bytes)

	_, ok := query.Query["$query"]
	if ok {
		return listDatabase(query, conn)
	}

	_, ok = query.Query["isMaster"]
	if ok {
		return isMaster(query, conn)
	}

	_, ok = query.Query["whatsmyuri"]
	if ok {
		return whatsmyuri(query, conn)
	}

	_, ok = query.Query["buildinfo"]
	if ok {
		return buildinfo(query, conn)
	}
	_, ok = query.Query["buildInfo"]
	if ok {
		return buildinfo(query, conn)
	}

	_, ok = query.Query["serverStatus"]
	if ok {
		return serverStatus(query, conn)
	}

	_, ok = query.Query["serverStatus"]
	if ok {
		return serverStatus(query, conn)
	}

	return defaultReply(query, conn)
}

func defaultReply(query *Query, w io.Writer) error {
	reply := NewReply(query.Header.RequestID)
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{"ok": 1}

	e := reply.Write(w)
	if e != nil {
		return e
	}

	return nil
}

func serverStatus(query *Query, w io.Writer) error {
	reply := NewReply(query.Header.RequestID)
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{"you": "118.114.245.36:48780", "ok": 1}

	e := reply.Write(w)
	if e != nil {
		return e
	}

	return nil
}

func buildinfo(query *Query, w io.Writer) error {
	reply := NewReply(query.Header.RequestID)
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{
		"version":          "3.4.0",
		"gitVersion":       "a4b751dcf51dd249c5865812b390cfd1c0129c30",
		"modules":          make([]string, 0),
		"allocator":        "tcmalloc",
		"javascriptEngine": "mozjs",
		"sysInfo":          "deprecated",
		"versionArray":     [4]int32{4, 2, 0, 0},
		"openssl": map[string]interface{}{
			"running":  "OpenSSL 1.1.1  11 Sep 2018",
			"compiled": "OpenSSL 1.1.1  11 Sep 2018",
		},
		"buildEnvironment": map[string]interface{}{
			"distmod":     "ubuntu1804",
			"distarch":    "x86_64",
			"cc":          "/opt/mongodbtoolchain/v3/bin/gcc: gcc (GCC) 8.2.0",
			"ccflags":     "-fno-omit-frame-pointer -fno-strict-aliasing -ggdb -pthread -Wall -Wsign-compare -Wno-unknown-pragmas -Winvalid-pch -Werror -O2 -Wno-unused-local-typedefs -Wno-unused-function -Wno-deprecated-declarations -Wno-unused-const-variable -Wno-unused-but-set-variable -Wno-missing-braces -fstack-protector-strong -fno-builtin-memcmp",
			"cxx":         "/opt/mongodbtoolchain/v3/bin/g++: g++ (GCC) 8.2.0",
			"cxxflags":    "-Woverloaded-virtual -Wno-maybe-uninitialized -fsized-deallocation -std=c++17",
			"linkflags":   "-pthread -Wl,-z,now -rdynamic -Wl,--fatal-warnings -fstack-protector-strong -fuse-ld=gold -Wl,--build-id -Wl,--hash-style=gnu -Wl,-z,noexecstack -Wl,--warn-execstack -Wl,-z,relro",
			"target_arch": "x86_64",
			"target_os":   "linux",
		},
		"bits":              64,
		"debug":             false,
		"maxBsonObjectSize": 16777216,
		"storageEngines": [4]string{
			"biggie",
			"devnull",
			"ephemeralForTest",
			"wiredTiger",
		},
		"ok": 1,
	}

	e := reply.Write(w)
	if e != nil {
		return e
	}

	return nil
}

func whatsmyuri(query *Query, w io.Writer) error {
	reply := NewReply(query.Header.RequestID)
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{"you": "118.114.245.36:48780", "ok": 1}

	e := reply.Write(w)
	if e != nil {
		return e
	}

	return nil
}

func isMaster(query *Query, writer io.Writer) error {
	reply := NewReply(query.Header.RequestID)
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{
		"ismaster":                     true,
		"maxBsonObjectSize":            16777216,
		"maxMessageSizeBytes":          48000000,
		"maxWriteBatchSize":            100000,
		"localTime":                    time.Now(),
		"logicalSessionTimeoutMinutes": 30,
		"connectionId":                 808,
		"minWireVersion":               0,
		"maxWireVersion":               3,
		"readOnly":                     false,
		"ok":                           1.0,
	}

	e := reply.Write(writer)
	if e != nil {
		return e
	}

	return nil
}

func listDatabase(query *Query, w io.Writer) error {
	reply := NewReply(query.Header.RequestID)
	reply.NumberReturned = 1
	reply.Documents = map[string]interface{}{
		"totalSize": 274432,
		"ok":        1,
		"databases": []interface{}{
			map[string]interface{}{
				"name":       "admin",
				"sizeOnDisk": 102400,
				"empty":      false,
			},
			map[string]interface{}{
				"name":       "config",
				"sizeOnDisk": 98304,
				"empty":      false,
			},
			map[string]interface{}{
				"name":       "local",
				"sizeOnDisk": 73728,
				"empty":      false,
			},
		},
	}
	e := reply.Write(w)
	if e != nil {
		return e
	}
	return nil
}

func TestByteWrite(t *testing.T) {
	buffer := &bytes.Buffer{}
	_ = binary.Write(buffer, binary.LittleEndian, int32(10))
	println(buffer.Len())
}
