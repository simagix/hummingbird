#!/bin/bash
docker rmi -f simagix/neutrino
id=$(docker create simagix/neutrino)
docker cp $id:/dist - | tar vx
docker rm $id
