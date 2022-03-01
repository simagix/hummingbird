// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"testing"
	"time"
)

func TestSimulate(t *testing.T) {
	client, err := GetMongoClient(TestSourceURI)
	assertEqual(t, nil, err)
	duration := 10 * time.Second
	sim := Simulator{client: client, duration: duration, TPS: 10}
	err = StartSimulation(sim)
	assertEqual(t, nil, err)
}
