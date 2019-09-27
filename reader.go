package mongo_protocol

import (
	"encoding/binary"
	"gopkg.in/mgo.v2/bson"
	"io"
)

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
