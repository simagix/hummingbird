// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package main

import (
	"fmt"
	"log"

	"github.com/simagix/hummingbird"
)

var repo = "neutrino"
var version = "devel"

func main() {
	vstring := fmt.Sprintf(`simagix/%v %v`, repo, version)
	if err := hummingbird.Neutrino(vstring); err != nil {
		log.Fatal(err)
	}
}
