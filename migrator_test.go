// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import "testing"

func TestNewMigratorInstance(t *testing.T) {
	filename := "testdata/minimum.json"
	m, err := NewMigratorInstance("dummy_filename")
	assertNotEqual(t, nil, err)

	m, err = NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	assertEqual(t, "all", m.Command)

	m = GetMigratorInstance()
	assertNotEqual(t, nil, m)
	assertEqual(t, "all", m.Command)
}

func TestValidateMigratorConfig(t *testing.T) {
	filename := "testdata/minimum.json"
	m, err := ReadMigratorConfig("dummy_filename")
	assertNotEqual(t, nil, err)

	m, err = ReadMigratorConfig(filename)
	assertEqual(t, nil, err)

	err = ValidateMigratorConfig(m)
	assertEqual(t, nil, err)
	assertEqual(t, "all", m.Command)
	assertEqual(t, "Apache-2.0", m.License)
	assertEqual(t, DefaultStaging, m.Staging)

	m.IsDrop = true
	m.Command = CommandData
	err = ValidateMigratorConfig(m)
	assertNotEqual(t, nil, err)

	m.Workers = MaxNumberWorkers + 1
	err = ValidateMigratorConfig(m)
	assertNotEqual(t, nil, err)

	m.Source = ""
	err = ValidateMigratorConfig(m)
	assertNotEqual(t, nil, err)

	m.Command = ""
	err = ValidateMigratorConfig(m)
	assertNotEqual(t, nil, err)
}
