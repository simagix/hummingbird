// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import "fmt"

// Resume resumes a migration
func Resume(filename string) error {
	inst, err := NewMigratorInstance(filename)
	if err != nil {
		return err
	}
	fmt.Println(inst.Command)
	return nil
}
