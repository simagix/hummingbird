// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"errors"
	"fmt"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
)

// Include stores namespace and query
type Include struct {
	Filter    bson.D   `bson:"filter,omitempty"`
	Limit     int64    `bson:"limit,omitempty"`
	Masks     []string `bson:"masks,omitempty"`
	Method    string   `bson:"method,omitempty"`
	Namespace string   `bson:"namespace"`
	To        string   `bson:"to,omitempty"`
}

// Includes stores Include
type Includes []*Include

func (p *Includes) String() string {
	str := `{ "includes": [`
	for i, include := range *p {
		if i > 0 {
			str += ","
		}
		data, _ := bson.MarshalExtJSON(include, false, false)
		str += fmt.Sprintf(`%v`, string(data))
	}
	return str + "] }"
}

// Set sets flag var
func (p *Includes) Set(value string) error {
	var err error
	var include *Include
	if include, err = GetInclude(value); err != nil {
		return err
	}
	*p = append(*p, include)
	return nil
}

// GetInclude returns Include
func GetInclude(value string) (*Include, error) {
	var err error
	var include = &Include{}
	if err = bson.UnmarshalExtJSON([]byte(value), false, include); err != nil {
		return include, err
	} else if include.Namespace == "" {
		return include, errors.New(`invalid namespace`)
	}

	if len(include.Filter) == 0 {
		include.Filter = bson.D{}
	}
	if len(include.Masks) > 0 { // mask fields
		if err = ConfigureMaskOption(include); err != nil {
			return include, err
		}
	}
	return include, err
}

// ConfigureMaskOption assigns mask option
func ConfigureMaskOption(include *Include) error {
	dbName, collName := mdb.SplitNamespace(include.Namespace)
	if dbName == "*" || collName == "*" {
		return fmt.Errorf(`%v, wildcard is not supported with masking`, include.Namespace)
	}
	if include.Method == "" {
		include.Method = MaskDefault
	} else if include.Method != MaskDefault && include.Method != MaskHEX && include.Method != MaskPartial {
		return fmt.Errorf(`invalid mask method %v`, include.Method)
	}
	return nil
}
