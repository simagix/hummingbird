// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package main

import (
	"fmt"
	"log"

	"github.com/simagix/humingbird"
)

var repo = "neutrino"
var version = "devel"

func main() {
	vstring := fmt.Sprintf(`simagix/%v %v`, repo, version)
	if err := humingbird.Run(vstring); err != nil {
		log.Fatal(err)
	}
}
