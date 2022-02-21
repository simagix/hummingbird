// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestDropMetaD(t *testing.T) {
	client, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)

	meta := &Workspace{}
	err = meta.DropMetaDB()
	assertNotEqual(t, nil, err)

	meta = &Workspace{db: client.Database(MetaDBName)}
	err = meta.DropMetaDB()
	assertEqual(t, nil, err)
}

func TestCleanUpWorkspace(t *testing.T) {
	meta := &Workspace{}
	err := meta.CleanUpWorkspace()
	assertNotEqual(t, nil, err)

	os.Mkdir(DefaultWorkspace, 0755)
	filename := DefaultWorkspace + "/replset.index"
	filenames := []string{DefaultWorkspace + "/file1.bson.gz", DefaultWorkspace + "/file2.bson.gz"}
	err = ioutil.WriteFile(filename, []byte(strings.Join(filenames, "\n")), 0644)
	assertEqual(t, true, DoesFileExist(filename))
	assertEqual(t, nil, err)
	meta = &Workspace{workspace: DefaultWorkspace}
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
	meta = &Workspace{workspace: DefaultWorkspace}
	err = meta.CleanUpWorkspace()
	assertEqual(t, nil, err)

	assertEqual(t, false, DoesFileExist(filename))
	for _, f := range filenames {
		assertEqual(t, false, DoesFileExist(f))
	}
}

func TestReset(t *testing.T) {
	client, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)

	meta := &Workspace{}
	err = meta.Reset()
	assertNotEqual(t, nil, err)

	meta = &Workspace{db: client.Database(MetaDBName)}
	err = meta.Reset()
	assertNotEqual(t, nil, err)

	meta = &Workspace{db: client.Database(MetaDBName), workspace: DefaultWorkspace}
	err = meta.Reset()
	assertEqual(t, nil, err)
}
