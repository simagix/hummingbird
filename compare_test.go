// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"testing"
)

func TestCompare(t *testing.T) {
	filename := "testdata/config.json"
	err := Compare(filename)
	assertEqual(t, nil, err)
}
