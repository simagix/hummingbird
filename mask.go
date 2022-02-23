// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	// MaskDefault uses default masking method
	MaskDefault = "default"
	// MaskHEX uses HEX masking method
	MaskHEX = "hex"
	// MaskPartial uses partial masking method
	MaskPartial = "partial"
)

// MaskFields mask all matched fields by traversing a doc
func MaskFields(doc *bson.D, fields []string, method string) {
	for _, field := range fields {
		elems := strings.Split(field, ".")
		maskDoc(*doc, elems, method)
	}
}

func maskDoc(doc bson.D, elems []string, method string) {
	elem := elems[0]
	elems = elems[1:]
	for i, v := range doc {
		if v.Key == elem {
			if len(elems) == 0 {
				doc[i].Value = getMaskedValue(doc[i].Value, method)
			} else if bsonD, ok := doc[i].Value.(bson.D); ok {
				maskDoc(bsonD, elems, method)
			} else if bsonA, ok := doc[i].Value.(bson.A); ok {
				for _, val := range bsonA {
					if bsonD, ok = val.(bson.D); ok {
						maskDoc(bsonD, elems, method)
					}
				}
			}
			return
		}
	}
}

func getMaskedValue(v interface{}, method string) interface{} {
	switch s := v.(type) {
	case string:
		if method == MaskPartial && len(s) > 4 {
			reg, _ := regexp.Compile("[a-zA-Z0-9]")
			return reg.ReplaceAllString(s[:len(s)-4], "X") + s[len(s)-4:]
		} else if method == MaskHEX {
			return primitive.NewObjectID().Hex()
		} else { // maskDefault
			reg, _ := regexp.Compile("[a-zA-Z0-9]")
			return reg.ReplaceAllString(s, "X")
		}
	default:
		return v
	}
}
