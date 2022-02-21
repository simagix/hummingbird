// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"io/ioutil"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestStartAll(t *testing.T) {
	filename := "testdata/quickstart.json"
	err := Start("none-exists")
	assertNotEqual(t, nil, err)

	m, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	m.Includes = nil
	SetMigratorInstance(m)
	err = DropCollections()
	assertEqual(t, nil, err)

	err = Start(filename)
	assertEqual(t, nil, err)
}

func TestStartConfig(t *testing.T) {
	filename := "testdata/config.json"
	err := Start("none-exists")
	assertNotEqual(t, nil, err)

	err = Start(filename)
	assertEqual(t, nil, err)
}

func TestStartIndex(t *testing.T) {
	filename := "testdata/index.json"
	err := Start("none-exists")
	assertNotEqual(t, nil, err)

	err = Start(filename)
	assertEqual(t, nil, err)
}

func TestStartDataOnly(t *testing.T) {
	filename := "testdata/data-only.json"
	err := Start("none-exists")
	assertNotEqual(t, nil, err)

	err = Start(filename)
	assertEqual(t, nil, err)

	m, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	m.IsDrop = true
	data, err := bson.MarshalExtJSON(m, false, false)
	assertEqual(t, nil, err)
	tmpfile := "temp-config.json"
	err = ioutil.WriteFile(tmpfile, data, 0644)
	assertEqual(t, nil, err)
	err = Start(tmpfile)
	assertNotEqual(t, nil, err)
	err = os.Remove(tmpfile)
	assertEqual(t, nil, err)
}

func TestDropCollections(t *testing.T) {
	filename := "testdata/quickstart.json"
	m, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)

	err = DropCollections()
	assertEqual(t, nil, err)

	m.Includes = nil
	SetMigratorInstance(m)
	err = DropCollections()
	assertEqual(t, nil, err)
}
