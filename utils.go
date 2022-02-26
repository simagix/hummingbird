// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// Stringify returns JSON string
func Stringify(doc interface{}) string {
	data, err := bson.MarshalExtJSON(doc, false, false)
	if err != nil {
		return ""
	}
	return string(data)
}

// DoesFileExist returns true if file exists
func DoesFileExist(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// ToFloat64 converts a value to float64
func ToFloat64(s interface{}) float64 {
	n, err := strconv.ParseFloat(fmt.Sprintf("%v", s), 64)
	if err != nil {
		return math.NaN()
	}
	return n
}

// ToInt32 converts a value to int32
func ToInt32(s interface{}) int32 {
	return int32(ToFloat64(s))
}

// ToInt64 converts a value to int64
func ToInt64(s interface{}) int64 {
	return int64(ToFloat64(s))
}

// RedactedURI redacted login and password
func RedactedURI(uri string) string {
	str := uri
	a := strings.Index(str, "://") + 3
	b := strings.LastIndex(str, "@")
	if b > 0 {
		str = str[:a] + "XXX:xxxxxx" + str[b:]
	}
	return str
}

// GetDateTime returns formatted date/time
func GetDateTime() string {
	t := time.Now()
	return fmt.Sprintf("%02d%02d%02d.%02d%02d%02d.%03d",
		t.Year()%100, t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1000/1000)
}
