#! /bin/bash
# Copyright Kuei-chun Chen, 2022-present. All rights reserved.
go test -v -coverprofile cover.out . 
go tool cover -html=cover.out -o cover.html