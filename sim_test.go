// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"testing"
	"time"
)

func TestSimulate(t *testing.T) {
	Simulate("testdata/sim.json")
}

func TestStartSimulation(t *testing.T) {
	client, err := GetMongoClient(TestSourceURI)
	assertEqual(t, nil, err)
	sim := Simulator{Namespaces: []string{"testdb.hummingbird", "testdb.neutrino"},
		client: client, duration: 10 * time.Second}
	sim.Threads.Find = 1
	sim.Threads.Insert = 1
	sim.Threads.Write = 1
	err = StartSimulation(sim)
	assertEqual(t, nil, err)
}
