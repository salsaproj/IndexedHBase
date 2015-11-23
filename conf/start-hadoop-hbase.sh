#!/bin/bash

ssh ln01 "start-dfs.sh; start-yarn.sh;"
sleep 30
ssh ln01 "mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver"
sleep 30
ssh ln02 "start-hbase.sh"
sleep 30
ssh ln02 "nohup hbase rest start > ~/rest.log &"
