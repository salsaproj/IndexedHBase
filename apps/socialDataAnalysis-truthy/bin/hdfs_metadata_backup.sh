#!/bin/bash

HDFS_BACKUP_DIR=/home/projects/yarn/backup/hdfs
HDFS_NAMENODE_DIR=/public/hadoop-2.5.1-dfs
USER=hadoop
TODAY=`date +"%Y-%m-%d"`
YESTERDAY=`date --date="1 day ago" +"%Y-%m-%d"`
THEDAYB4YESTERDAY=`date --date="2 days ago" +"%Y-%m-%d"`
TMP_DIR_PATH1=${HDFS_BACKUP_DIR}/hadoop-2.5.1-dfs-${TODAY}

# run on ln01 as hadoop user

cp -R ${HDFS_NAMENODE_DIR} ${TMP_DIR_PATH1}
if [[ $? != 0 ]]; then
    echo "error when copying hadoop metadata from namenode" 1>&2;
    exit 1;
fi

# remove the old backups, and keep the latest three backup
pushd ${HDFS_BACKUP_DIR} 1>/dev/null

for dir in */ ; do
    if [[ ${dir} == *"${TODAY}"* ]]; then    
        # do nothing
        echo "keep ${dir}"
    elif [[ ${dir} == *"${YESTERDAY}"* ]]; then
        echo "keep ${dir}"  
    elif [[ ${dir} == *"${THEDAYB4YESTERDAY}"* ]]; then
        echo "keep ${dir}"
    else 
        echo "remove old backup ${dir}"
        if [ ! -z "${dir}" -a "${dir}" != " " ]; then
            rm -rf ${dir}
        fi
    fi
done

popd 1>/dev/null