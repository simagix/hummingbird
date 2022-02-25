// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestCachedOplogs(t *testing.T) {
	filename := TestAllDataTypesFile
	var data []byte
	var oplogs []Oplog
	reader, err := NewBSONReader(filename)
	assertEqual(t, nil, err)
	for {
		if data = reader.Next(); data == nil {
			break
		}
		var oplog Oplog
		if err = bson.Unmarshal(data, &oplog); err != nil {
			t.Fatal(err)
		}
		oplogs = append(oplogs, oplog)
	}

	ofile := filepath.Base(filename)
	WriteCachedOplogs(oplogs, ofile)
	noplogs, err := ReadCachedOplogs(ofile)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, len(oplogs), len(noplogs))
	sort.Slice(oplogs, func(i int, j int) bool {
		return Stringify(oplogs[i]) < Stringify(oplogs[j])
	})
	sort.Slice(noplogs, func(i int, j int) bool {
		return Stringify(noplogs[i]) < Stringify(noplogs[j])
	})
	for i := 0; i < len(oplogs); i++ {
		assertEqual(t, true, reflect.DeepEqual(oplogs[i], noplogs[i]))
	}
	os.Remove(ofile)
}
