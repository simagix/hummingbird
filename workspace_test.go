// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestDropMetaDB(t *testing.T) {
	meta := &Workspace{}
	err := meta.DropMetaDB()
	assertNotEqual(t, nil, err)

	meta = &Workspace{dbName: MetaDBName, dbURI: TestTargetURI}
	err = meta.DropMetaDB()
	assertEqual(t, nil, err)
}

func TestCleanUpWorkspace(t *testing.T) {
	meta := &Workspace{}
	err := meta.CleanUpWorkspace()
	assertNotEqual(t, nil, err)

	os.Mkdir(DefaultStaging, 0755)
	filename := DefaultStaging + "/replset.index"
	filenames := []string{DefaultStaging + "/file1.bson.gz", DefaultStaging + "/file2.bson.gz"}
	err = ioutil.WriteFile(filename, []byte(strings.Join(filenames, "\n")), 0644)
	assertEqual(t, true, DoesFileExist(filename))
	assertEqual(t, nil, err)
	meta = &Workspace{staging: DefaultStaging}
	err = meta.CleanUpWorkspace()
	assertNotEqual(t, nil, err)

	for _, f := range filenames {
		err = ioutil.WriteFile(f, []byte(f), 0644)
		assertEqual(t, nil, err)
		assertEqual(t, true, DoesFileExist(f))
	}
	err = ioutil.WriteFile(filename, []byte(strings.Join(filenames, "\n")), 0644)
	assertEqual(t, true, DoesFileExist(filename))
	assertEqual(t, nil, err)
	meta = &Workspace{staging: DefaultStaging}
	err = meta.CleanUpWorkspace()
	assertEqual(t, nil, err)

	assertEqual(t, false, DoesFileExist(filename))
	for _, f := range filenames {
		assertEqual(t, false, DoesFileExist(f))
	}
}

func TestReset(t *testing.T) {
	meta := &Workspace{}
	err := meta.Reset()
	assertNotEqual(t, nil, err)

	meta = &Workspace{dbName: MetaDBName, dbURI: TestTargetURI}
	err = meta.Reset()
	assertNotEqual(t, nil, err)

	meta = &Workspace{dbName: MetaDBName, dbURI: TestTargetURI, staging: DefaultStaging}
	err = meta.Reset()
	assertEqual(t, nil, err)
}
