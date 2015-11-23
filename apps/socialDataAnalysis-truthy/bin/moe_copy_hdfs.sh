#!/bin/bash
DIR=/home/data/truthy_tmp
DESTDIR_ON_MOE=/home/appuser/truthy_tmp
DESTDIR_ON_HDFS=/truthy/data
REMOTE_USER=appuser
HOST=moe.soic.indiana.edu
KEY=${HOME}/.ssh/id_rsa_moe

# # uncomment this block to set debug mode on 
# set -x

# terminate script on first error
set -e

# create SSH agent and add key
eval `ssh-agent -s` 1>/dev/null
ssh-add ${KEY} 2>/dev/null

pushd ${DIR} 1>/dev/null
LATEST_GZ=`find ${DIR} -type f -mtime 0 -name "*.json.gz" -printf "%f"`
rsync -aq --ignore-existing ${LATEST_GZ} ${REMOTE_USER}@${HOST}:${DESTDIR_ON_MOE}
popd 1>/dev/null

# upload file on HDFS
ssh ${REMOTE_USER}@${HOST} "hadoop fs -copyFromLocal -f ${DESTDIR_ON_MOE}/${LATEST_GZ} ${DESTDIR_ON_HDFS}/${LATEST_GZ}"

# remove the tmp file
ssh ${REMOTE_USER}@${HOST} "rm -f ${DESTDIR_ON_MOE}/${LATEST_GZ}"

# write the latest json.gz filename on MOE status
ssh ${REMOTE_USER}@${HOST} "echo ${LATEST_GZ} > ${DESTDIR_ON_MOE}/latestJsonGz"

# kill SSH agent
ssh-agent -k 1>/dev/null

# set +x
