// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	data = `{"ssn": "555-66-7878", 
	"level2": {"ssn": "555-66-7878"}, 
	"array": [{"ssn": "555-66-7878"}, {"ssn": "555-66-7878"}],
	"strings": ["a", "b", "c"],
	"numbers": [1, 2, 3]}`
)

func TestMaskFields(t *testing.T) {
	var doc bson.D
	err := bson.UnmarshalExtJSON([]byte(data), false, &doc)
	assertEqual(t, nil, err)
	MaskFields(&doc, []string{"ssn"}, MaskDefault)
	m := doc.Map()
	assertEqual(t, "XXX-XX-XXXX", m["ssn"])
}

func TestMaskFieldsSubDocument(t *testing.T) {
	var doc bson.D
	err := bson.UnmarshalExtJSON([]byte(data), false, &doc)
	assertEqual(t, nil, err)
	MaskFields(&doc, []string{"level2.ssn"}, MaskDefault)
	m := doc.Map()
	assertEqual(t, "XXX-XX-XXXX", m["level2"].(bson.D).Map()["ssn"])
}

func TestMaskFieldsDocumentArray(t *testing.T) {
	var doc bson.D
	err := bson.UnmarshalExtJSON([]byte(data), false, &doc)
	assertEqual(t, nil, err)
	MaskFields(&doc, []string{"array.ssn"}, MaskDefault)
	m := doc.Map()
	for _, v := range m["array"].(bson.A) {
		assertEqual(t, "XXX-XX-XXXX", v.(bson.D).Map()["ssn"])
	}
}

func TestMaskFieldsPrimitiveArray(t *testing.T) {
	var doc bson.D
	err := bson.UnmarshalExtJSON([]byte(data), false, &doc)
	assertEqual(t, nil, err)
	MaskFields(&doc, []string{"strings"}, MaskDefault)
	m := doc.Map()
	assertEqual(t, 3, len(m["strings"].(primitive.A)))
}

func TestGetMaskedValueDefault(t *testing.T) {
	s := "555-66-7878"
	assertEqual(t, getMaskedValue(s, MaskDefault), "XXX-XX-XXXX")
}

func TestGetMaskedValuePartial(t *testing.T) {
	s := "555-66-7878"
	assertEqual(t, getMaskedValue(s, MaskPartial), "XXX-XX-7878")
}

func TestGetMaskedValueUnique(t *testing.T) {
	s := "555-66-7878"
	ret := getMaskedValue(s, MaskHEX).(string)
	assertEqual(t, len(ret), 24)
}
