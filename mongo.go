package mongo_protocol

import (
	"bytes"
	"encoding/binary"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
)

/*
struct Section {
    uint8 payloadType;
    union payload {
        document  document; // payloadType == 0
        struct sequence { // payloadType == 1
            int32      size;
            cstring    identifier;
            document*  documents;
        };
    };
};

struct OP_MSG {
    struct MsgHeader {
        int32  messageLength;
        int32  requestID;
        int32  responseTo;
        int32  opCode = 2013;
    };
    uint32      flagBits;
    Section+    sections;
    [uint32     checksum;]
};
*/

type MsgReply struct {
	*Msg
	Header *MsgHeader
}

func NewMsgReply(requestID int32) *MsgReply {
	return &MsgReply{
		Header: &MsgHeader{
			MessageLength: 0,
			ResponseTo:    requestID,
			OpCode:        OP_MSG,
		},
		Msg: &Msg{
			Sections: make([]MsgSection, 0),
		},
	}
}

func (m *MsgReply) Write(w io.Writer) error {
	buffer := &bytes.Buffer{}
	if e := binary.Write(buffer, binary.LittleEndian, m.FlatBits); e != nil {
		return e
	}
	for _, v := range m.Sections {
		if _, e := buffer.Write([]byte{v.GetKind()}); e != nil {
			return e
		}
		body, ok := v.(*BodyMsgSection)
		if ok {
			if out, e := bson.Marshal(body.Body); e != nil {
				return e
			} else {
				if _, e = buffer.Write(out); e != nil {
					return e
				}
			}
		} else {
			section := v.(*DocumentSequenceMsgSection)
			if e := binary.Write(buffer, binary.LittleEndian, section.Size); e != nil {
				return e
			}
			if _, e := buffer.Write([]byte(section.DocumentSequenceIdentifier)); e != nil {
				return e
			}
			for _, doc := range section.DocumentSequences {
				if out, e := bson.Marshal(doc); e != nil {
					return e
				} else {
					if _, e = buffer.Write(out); e != nil {
						return e
					}
				}
			}
		}
	}

	m.Header.MessageLength = int32(4*4 + buffer.Len())
	if e := binary.Write(w, binary.LittleEndian, m.Header); e != nil {
		return e
	}
	if _, e := w.Write(buffer.Bytes()); e != nil {
		return e
	}
	return nil
}

type DocumentSequenceMsgSection struct {
	Kind                       byte
	Size                       int32
	DocumentSequenceIdentifier string
	DocumentSequences          []bson.M
}

func NewDocumentSequenceMsgSection() *DocumentSequenceMsgSection {
	return &DocumentSequenceMsgSection{Kind: 1}
}

func (d *DocumentSequenceMsgSection) GetKind() byte {
	return d.Kind
}

type BodyMsgSection struct {
	Kind byte
	Body bson.M
}

func NewBodyMsgSection() *BodyMsgSection {
	return &BodyMsgSection{Kind: 0}
}

func (b *BodyMsgSection) GetKind() byte {
	return b.Kind
}

type Msg struct {
	FlatBits uint32
	Sections []MsgSection
}

type MsgSection interface {
	GetKind() byte
}

func (m *Msg) UnMarshal(r *Reader) error {
	m.Sections = make([]MsgSection, 0)
	defer func() {
		_, _ = ioutil.ReadAll(r)
	}()
	flat, e := r.ReadInt32()
	if e != nil {
		return e
	}
	m.FlatBits = uint32(flat)
	for {
		kindBytes, e := r.ReadBytes(1)
		if kindBytes == nil || e != nil {
			break
		}
		kind := kindBytes[0]
		switch kind {
		case 0:
			document, e := r.ReadDocument()
			if e != nil {
				return e
			}
			m.Sections = append(m.Sections, &BodyMsgSection{
				Kind: kind,
				Body: document,
			})
		case 1:
			size, e := r.ReadInt32()
			if e != nil {
				return e
			}
			reader := io.LimitReader(r, int64(size))
			secReader := &Reader{reader}

			ident, e := secReader.ReadCString()
			if e != nil {
				return e
			}
			documents, e := secReader.ReadDocuments()
			if e != nil {
				return e
			}
			m.Sections = append(m.Sections, &DocumentSequenceMsgSection{
				Kind:                       kind,
				Size:                       size,
				DocumentSequenceIdentifier: ident,
				DocumentSequences:          documents,
			})
		}
	}
	return nil
}

func (m *Msg) GetBodyMsgSection() bson.M {
	for _, v := range m.Sections {
		if v.GetKind() == 0 {
			return v.(*BodyMsgSection).Body
		}
	}
	return nil
}

type KillCursors struct {
	// standard message header
	Header MsgHeader
	// 0 - reserved for future use
	ZERO int32
	// number of cursorIDs in message
	NumberOfCursorIDs int32
	// sequence of cursorIDs to close
	CursorIDs []int64
}

func (k *KillCursors) UnMarshal(r *Reader) error {
	n, e := r.ReadInt32()
	k.ZERO = n
	i, e := r.ReadInt32()
	k.NumberOfCursorIDs = i

	var cursorIDs []int64
	for {
		id, e := r.ReadInt64()
		if e != nil {
			break
		}
		cursorIDs = append(cursorIDs, *id)
	}
	k.CursorIDs = cursorIDs
	if e == io.EOF {
		return nil
	}
	return e
}

type Delete struct {
	// standard message header
	header MsgHeader
	// 0 - reserved for future use
	ZERO int32
	// "dbname.collectionname"
	FullCollectionName string
	// bit vector - see below for details.
	Flags int32
	// query object.  See below for details.
	Selector bson.M
}

func (d *Delete) UnMarshal(r *Reader) error {
	n, e := r.ReadInt32()
	d.ZERO = n
	s, e := r.ReadCString()
	d.FullCollectionName = s
	i, e := r.ReadInt32()
	d.Flags = i
	m, e := r.ReadDocument()
	d.Selector = m
	if e == io.EOF {
		return nil
	}
	return e
}

type GetMore struct {
	// 0 - reserved for future use
	ZERO int32
	// "dbname.collectionname"
	FullCollectionName string
	// number of documents to return
	NumberToReturn int32
	// cursorID from the OP_REPLY
	CursorID *int64
}

func (g *GetMore) UnMarshal(r *Reader) error {
	n, e := r.ReadInt32()
	g.ZERO = n
	s, e := r.ReadCString()
	g.FullCollectionName = s
	i, e := r.ReadInt32()
	g.NumberToReturn = i
	readInt64, e := r.ReadInt64()
	g.CursorID = readInt64
	if e == io.EOF {
		return nil
	}
	return e
}

type Query struct {
	// standard message header
	Header MsgHeader
	// bit vector of query options.  See below for details.
	Flags int32
	// "dbname.collectionname"
	FullCollectionName string
	// number of documents to skip
	NumberToSkip int32
	// number of documents to return
	NumberToReturn int32
	//  in the first OP_REPLY batch
	// query object.  See below for details.
	Query bson.M
	// Optional. Selector indicating the fields
	ReturnFieldsSelector bson.M
	//  to return.  See below for details.
}

func (q *Query) UnMarshal(r *Reader) error {
	n, e := r.ReadInt32()
	q.Flags = n
	s, e := r.ReadCString()
	q.FullCollectionName = s
	i, e := r.ReadInt32()
	q.NumberToSkip = i
	n2, e := r.ReadInt32()
	q.NumberToReturn = n2
	m, e := r.ReadDocument()
	q.Query = m
	ms, e := r.ReadDocument()
	q.ReturnFieldsSelector = ms
	if e == io.EOF {
		return nil
	}
	return e
}

type Insert struct {
	// standard message header
	Header MsgHeader
	// bit vector - see below
	Flags int32
	// "dbname.collectionname"
	FullCollectionName string
	// one or more documents to insert into the collection
	Documents []bson.M
}

func (i *Insert) UnMarshal(r *Reader) error {
	n, e := r.ReadInt32()
	i.Flags = n
	s, e := r.ReadCString()
	i.FullCollectionName = s
	ms, e := r.ReadDocuments()
	i.Documents = ms
	if e == io.EOF {
		return nil
	}
	return e
}

type Update struct {
	// standard message header
	Header MsgHeader
	// 0 - reserved for future use
	ZERO int32
	// "dbname.collectionname"
	FullCollectionName string
	// bit vector. see below
	Flags int32
	// the query to select the document
	Selector bson.M
	// specification of the update to perform
	Update bson.M
}

func (u *Update) UnMarshal(r *Reader) error {
	z, e := r.ReadInt32()
	u.ZERO = z
	s, e := r.ReadCString()
	u.FullCollectionName = s
	n, e := r.ReadInt32()
	u.Flags = n
	m, e := r.ReadDocument()
	u.Selector = m
	ms, e := r.ReadDocument()
	u.Update = ms
	if e == io.EOF {
		return nil
	}
	return e
}

type UnMarshaler interface {
	UnMarshal(r *Reader) error
}

type Writer interface {
	Write(w io.Writer) error
}

type MsgHeader struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        OpCode
}

type Reply struct {
	// standard message header
	Header *MsgHeader
	// bit vector - see details below
	ResponseFlags ResponseFlags
	// cursor id if client needs to do get more's
	CursorID int64
	// where in the cursor this reply is starting
	StartingFrom int32
	// number of documents in the reply
	NumberReturned int32
	// documents
	Documents interface{}
}

func NewReply(requestId int32) *Reply {
	msgHeader := &MsgHeader{
		OpCode:     OP_REPLY,
		ResponseTo: requestId,
	}
	return &Reply{
		Header:    msgHeader,
		Documents: make([]interface{}, 0),
	}
}

/*
1,计算header中字节大小
2,依次按照小端序写入w
*/
func (r *Reply) Write(w io.Writer) error {
	out, e := bson.Marshal(r.Documents)
	if e != nil {
		return e
	}
	dataLen := 4*4 + 4 + 8 + 4 + 4 + len(out)
	r.Header.MessageLength = int32(dataLen)
	data := []interface{}{r.Header, r.ResponseFlags, r.CursorID, r.StartingFrom, r.NumberReturned}
	for _, v := range data {
		e = binary.Write(w, binary.LittleEndian, v)
		if e != nil {
			return e
		}
	}
	_, e = w.Write(out)
	if e != nil {
		return e
	}
	return nil
}

type ResponseFlags int32

const (
	CursorNotFound ResponseFlags = iota
	QueryFailure
	ShardConfigStale
	AwaitCapable
)

type OpCode int32

const (
	OP_REPLY        OpCode = 1
	OP_UPDATE       OpCode = 2001
	OP_INSERT       OpCode = 2002
	RESERVED        OpCode = 2003
	OP_QUERY        OpCode = 2004
	OP_GET_MORE     OpCode = 2005
	OP_DELETE       OpCode = 2006
	OP_KILL_CURSORS OpCode = 2007
	OP_MSG          OpCode = 2013
)
