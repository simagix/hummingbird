// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import "testing"

func TestResume(t *testing.T) {
	filename := "testdata/quickstart.json"
	err := Resume("none-exists")
	assertNotEqual(t, nil, err)

	err = Resume(filename, true)
	assertEqual(t, nil, err)
}
