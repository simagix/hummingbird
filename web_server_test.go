// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"net/http"
	"testing"
	"time"
)

func TestStartWebServer(t *testing.T) {
	go StartWebServer(Port)

	time.Sleep(1 * time.Second)
	client := http.Client{Timeout: time.Duration(1) * time.Second}
	_, err := client.Get("http://localhost:3629/")
	assertEqual(t, nil, err)

	_, err = client.Get("http://localhost:3629/favicon.ico")
	assertEqual(t, nil, err)
}
