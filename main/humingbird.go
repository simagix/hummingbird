// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package main

import (
	"fmt"

	"github.com/simagix/humingbird"
)

var repo = "simagix/neutrino"
var version = "devel"

func main() {
	vstring := fmt.Sprintf(`%v %v`, repo, version)
	humingbird.Run(vstring)
}
