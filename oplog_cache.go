// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"bytes"
	"fmt"

	"github.com/simagix/gox"

	"go.mongodb.org/mongo-driver/bson"
)

// WriteCachedOplogs writes oplogs to a file
func WriteCachedOplogs(oplogs []Oplog, filename string) error {
	var err error
	var buffer bytes.Buffer
	for _, oplog := range oplogs {
		var data []byte
		if data, err = bson.Marshal(oplog); err != nil {
			return fmt.Errorf("Marshal failed: %v", err)
		}
		nw := 0
		var n int
		for nw < len(data) {
			if n, err = buffer.Write(data[nw:]); err != nil {
				return fmt.Errorf("Write failed: %v", err)
			}
			nw += n
		}
	}
	return gox.OutputGzipped(buffer.Bytes(), filename)
}

// ReadCachedOplogs writes oplogs to a file
func ReadCachedOplogs(filename string) ([]Oplog, error) {
	var err error
	var data []byte
	var oplogs []Oplog
	var reader *BSONReader
	if reader, err = NewBSONReader(filename); err != nil {
		return nil, fmt.Errorf("NewBSONReader failed: %v", err)
	}
	for {
		if data = reader.Next(); data == nil {
			break
		}
		var oplog Oplog
		if err = bson.Unmarshal(data, &oplog); err != nil {
			return nil, fmt.Errorf("Unmarshal failed: %v", err)
		}
		oplogs = append(oplogs, oplog)
	}
	return oplogs, nil
}
