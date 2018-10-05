#!/bin/bash

docker run -d --name "sandbox-kafka" --hostname "sandbox-kafka" --net test-net --ip 172.18.0.10 sandbox-kafka
