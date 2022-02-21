// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import "testing"

func TestStartAll(t *testing.T) {
	filename := "testdata/quickstart.json"
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
