// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import "testing"

func TestNewMigratorInstance(t *testing.T) {
	filename := "testdata/minimum.json"
	inst, err := NewMigratorInstance("dummy_filename")
	assertNotEqual(t, nil, err)

	inst, err = NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	assertEqual(t, "all", inst.Command)

	inst = GetMigratorInstance()
	assertNotEqual(t, nil, inst)
	assertEqual(t, "all", inst.Command)
}

func TestValidateMigratorConfig(t *testing.T) {
	filename := "testdata/minimum.json"
	inst, err := ReadMigratorConfig("dummy_filename")
	assertNotEqual(t, nil, err)

	inst, err = ReadMigratorConfig(filename)
	assertEqual(t, nil, err)

	err = ValidateMigratorConfig(inst)
	assertEqual(t, nil, err)
	assertEqual(t, "all", inst.Command)
	assertEqual(t, "Apache-2.0", inst.License)
	assertEqual(t, DefaultSpool, inst.Spool)

	inst.IsDrop = true
	inst.Command = CommandData
	err = ValidateMigratorConfig(inst)
	assertNotEqual(t, nil, err)

	inst.Workers = MaxNumberWorkers + 1
	err = ValidateMigratorConfig(inst)
	assertNotEqual(t, nil, err)

	inst.Source = ""
	err = ValidateMigratorConfig(inst)
	assertNotEqual(t, nil, err)

	inst.Command = ""
	err = ValidateMigratorConfig(inst)
	assertNotEqual(t, nil, err)
}

func TestSkipNamespace(t *testing.T) {
	filename := "testdata/config.json"
	inst, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	inst.included = map[string]*Include{}
	inst.included["db.*"] = &Include{Namespace: "db.*"}
	inst.included["*.coll"] = &Include{Namespace: "*.coll"}
	inst.included["dbname.collname"] = &Include{Namespace: "dbname.collname"}

	assertEqual(t, false, inst.SkipNamespace("dbname.collname"))
	assertEqual(t, false, inst.SkipNamespace("db.collection"))
	assertEqual(t, false, inst.SkipNamespace("database.coll"))
}
