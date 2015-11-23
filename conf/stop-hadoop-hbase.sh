#!/bin/bash

ssh ln02 "stop-hbase.sh"
ssh ln01 "stop-yarn.sh"
ssh ln01 "stop-dfs.sh"
