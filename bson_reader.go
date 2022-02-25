// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"io"

	"github.com/simagix/gox"
)

// BSONReader stores bson reader info
type BSONReader struct {
	Stream io.ReadCloser
}

// NewBSONReader returns a bson reader
func NewBSONReader(filename string) (*BSONReader, error) {
	reader, err := gox.NewFileReader(filename)
	if err != nil {
		return nil, err
	}
	return &BSONReader{Stream: io.NopCloser(reader)}, err
}

// Next returns next bson doc
func (p *BSONReader) Next() []byte {
	var err error
	var header = make([]byte, 4)
	if _, err = io.ReadAtLeast(p.Stream, header, 4); err != nil {
		return nil
	}
	size := int32((uint32(header[0]) << 0) | (uint32(header[1]) << 8) |
		(uint32(header[2]) << 16) | (uint32(header[3]) << 24),
	)
	if size > 16*mb || size < 5 {
		return nil
	}
	var data = make([]byte, size)
	copy(data, header)
	if _, err = io.ReadAtLeast(p.Stream, data[4:], int(size-4)); err != nil {
		return nil
	}
	return data
}
