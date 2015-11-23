#!/bin/bash
# TODO currently this script runs on headnode/login node, 
# it may cause OOM or CPU overloading problems when the sort-uid operation
# uses all the resource.

# this script relies on the cron job setup on MOE as appuser 

# Usage: 1. load and index for yesterday json file ./daily_loading_moe.sh 
#        2. manual load and index for specified json file ./daily_loading_moe.sh 2015-03-02.json.gz

# trap "echo \"daily_loading_moe.sh failed, please login to MOE and see details.\" | mail -s \"daily_loading_moe.sh failed\" taklwu@indiana.edu,truthy@indiana.edu" ERR

TRUTHY_CORE_DIR=/home/hadoop/software/IndexedHBase-CoreTruthy-0.2
TRUTHY_CMD_DIR=${TRUTHY_CORE_DIR}/bin
LOCAL_UIDS_DIR=${HOME}/uids
DESTDIR_ON_MOE=/home/projects/truthy_tmp
DESTDIR_ON_HDFS=/truthy/data
DESTDIR_ON_CN01=/data/sdl/public/hadoop-2.5.1-tmp/daily/
HOST_CN01=cn01
# our source code loads from /truthy/loading/
TMP_DIR_ON_HDFS=daily
USER=appuser
HOST=moe.soic.indiana.edu
HBASE_REST_HTTP_HOST=http://moe-ln02.soic.indiana.edu
HBASE_REST_PORT=8080
REGIONS=10
# we can change this number when needed
TWEETS_PER_DAY=40000000
YESTERDAY=`date -d "yesterday 13:00 " '+%Y-%m-%d'`
LATEST_GZ=${YESTERDAY}".json.gz"
REGULAR_LOAD=0

source ${HOME}/.bash_profile

# # uncomment this block to set debug mode on 
# set -x

# terminate script on first error
# set -e

# check if this script manually starts with a json gz file
if [ ! -z "$1" ]; then 
    echo "Start with a *.json.gz file: $1"
    LATEST_GZ=$1
    REGULAR_LOAD=1
fi

JSON_FILENAME="${LATEST_GZ%.gz}"
# make sort-uids.gz, year, month, day from passing filename
SORT_GZ=`echo ${LATEST_GZ} | awk -F '-' '{print $1$2}'`"-uids.gz"
YEAR_OF_GZ=`echo ${LATEST_GZ} | awk -F '-' '{print $1}'`
MONTH_OF_GZ=`echo ${LATEST_GZ} | awk -F '-' '{print $2}'`
DAY_OF_GZ=`echo ${LATEST_GZ} | awk -F '-' '{print $3}' | awk -F '.' '{print $1}'`
CUSTOM_INDEX_XML_PATH=${TRUTHY_CORE_DIR}/conf/custom-index-${YEAR_OF_GZ}-${MONTH_OF_GZ}.xml

logger -t truthy-daily-loading "Started loading ${LATEST_GZ}"
echo "Started loading ${LATEST_GZ}" > ${DESTDIR_ON_MOE}/daily_loading.log

## Functions Declaration ##
# check if this is the first day of month, assuming data are all store on HDFS
isFirstDay() 
{
    # $1 = $LATEST_GZ, $2= $YEAR_OF_GZ-$MONTH_OF_GZ
    # get the first day data on HDFS, assume this line won't get error
    FIRST_ROW=`hadoop fs -ls ${DESTDIR_ON_HDFS}/${2}* 2>&1 | sed -n 1p`
    echo "first day of month = ${FIRST_ROW}"
    if [[ ${FIRST_ROW} == *"${1}" ]]; then 
        return 0;
    else
        return 1;
    fi
}

fileExists() 
{
    # $1 path to file
    if [ -f $1 ]; then
        echo "GZ File found on LOCAL DISK: $1";
	return 0;
    else
        echo "GZ File not found on LOCAL DISK: $1"
        return 1;
    fi
}

isTableExist() {
    CMD=`ssh ln02 curl ${HBASE_REST_HTTP_HOST}:${HBASE_REST_PORT}/$1/exists 2>&1`
    logger -t truthy-daily-loading "ssh ln02 curl ${HBASE_REST_HTTP_HOST}:${HBASE_REST_PORT}/$1/exists 2>&1"
    # $1 tableName
    if [[ ${CMD} = *"Not found"* ]]; then 
        echo "HBase Table $1 Not Found"
        return 1;
    else 
        echo "HBase Table $1 Found"
        return 0;
    fi    
}

## Start Data loading and indexing ##
# before start, check if file exists on hdfs with 8 retries every 1 hour.
# we check the first line of ${DESTDIR_ON_MOE}/latestJsonGz updated remotely by moe_copy_hdfs.sh
i=1
logger -t truthy-daily-loading "check if file ${DESTDIR_ON_MOE}/latestJsonGz contains ${LATEST_GZ}"
echo "check if file ${DESTDIR_ON_MOE}/latestJsonGz contains ${LATEST_GZ}" >> ${DESTDIR_ON_MOE}/daily_loading.log

while [ ${i} -lt 9 ] ;
do
    # check if this is manual load 
    if [ ${REGULAR_LOAD} -eq 1 ]; then
        echo "Manual load turns on, make sure *.json.gz exists on HDFS."
        break;
    fi
    
    # check if file exist on HDFS
    latestJson=`head -n 1 ${DESTDIR_ON_MOE}/latestJsonGz`
    if [[ ${latestJson} = "${LATEST_GZ}" ]]; then 
        break;
    else 
        echo "${i} attempt(s), ${DESTDIR_ON_HDFS}/${LATEST_GZ} does not exist, sleep for 1 hour"
        sleep 1h;
    fi
    
    i=$[${i}+1]
    if [ ${i} -eq 9 ]; then
        echo "Finish all 8 retries, exit with error"
        exit 1
    fi
done

logger -t truthy-daily-loading "After getting data from SMITHERS ${LATEST_GZ}"
echo "After getting data from SMITHERS ${LATEST_GZ}" >> ${DESTDIR_ON_MOE}/daily_loading.log

# if this is the first day of month, make xml sort-uids and create-tables
if isFirstDay ${LATEST_GZ} ${YEAR_OF_GZ}"-"${MONTH_OF_GZ} || ! isTableExist "userTable-"${YEAR_OF_GZ}"-"${MONTH_OF_GZ} ; then 
    # make xml file
    sed -e s/__year__/${YEAR_OF_GZ}/g -e s/__month__/${MONTH_OF_GZ}/g ${TRUTHY_CORE_DIR}/conf/custom-index.xml.tpl > ${CUSTOM_INDEX_XML_PATH}

    if ! fileExists ${DESTDIR_ON_MOE}/${LATEST_GZ}; then
        echo "hadoop fs -copyToLocal ${DESTDIR_ON_HDFS}/${LATEST_GZ} ${DESTDIR_ON_MOE}/"
        mkdir -p ${DESTDIR_ON_MOE}
        hadoop fs -copyToLocal ${DESTDIR_ON_HDFS}/${LATEST_GZ} ${DESTDIR_ON_MOE}/ 2>&1
    fi
    
    # sort-uids
    mkdir -p ${LOCAL_UIDS_DIR}
    pushd ${TRUTHY_CMD_DIR} 1>/dev/null
        echo "./truthy-cmd.sh sort-uid ${DESTDIR_ON_MOE}/${LATEST_GZ} ${LOCAL_UIDS_DIR}/${SORT_GZ}"
        ./truthy-cmd.sh sort-uid ${DESTDIR_ON_MOE}/${LATEST_GZ} ${LOCAL_UIDS_DIR}/${SORT_GZ} 2>&1
        chmod 755 ${LOCAL_UIDS_DIR}/${SORT_GZ}

    # create table for this month
        echo "./truthy-cmd.sh create-tables ${YEAR_OF_GZ}-${MONTH_OF_GZ} ${REGIONS} ${LOCAL_UIDS_DIR}/${SORT_GZ}"
       ./truthy-cmd.sh create-tables ${YEAR_OF_GZ}-${MONTH_OF_GZ} ${REGIONS} ${LOCAL_UIDS_DIR}/${SORT_GZ} 2>&1
    popd 1>/dev/null
else 
    echo "It's not first day, start daily loading soon"
fi

## Daily load and index ##
logger -t truthy-daily-loading "Running data loader ${LATEST_GZ}"
echo "Running data loader ${LATEST_GZ}" >> ${DESTDIR_ON_MOE}/daily_loading.log

# check if file exist on HDFS
logger -t truthy-daily-loading "hadoop fs -test -e ${DESTDIR_ON_HDFS}/${LATEST_GZ}"
hadoop fs -test -e ${DESTDIR_ON_HDFS}/${LATEST_GZ}

# check if json file exist on LOCAL
logger -t truthy-daily-loading "check if json file exist on LOCAL: ${DESTDIR_ON_MOE}/${LATEST_GZ}"
if ! fileExists ${DESTDIR_ON_MOE}/${LATEST_GZ} ; then
    mkdir -p ${DESTDIR_ON_MOE}
    logger -t truthy-daily-loading "hadoop fs -copyToLocal ${DESTDIR_ON_HDFS}/${LATEST_GZ} ${DESTDIR_ON_MOE}/"
    hadoop fs -copyToLocal ${DESTDIR_ON_HDFS}/${LATEST_GZ} ${DESTDIR_ON_MOE}/ 2>&1
fi

## split a single file to 64 splits on cn01
eval `ssh-agent -s` 1>/dev/null

# move files to cn01 /data/sdl/public/hadoop-2.5.1-tmp/daily/, this steps takes 20+ mins
logger -t truthy-daily-loading "rsync ${DESTDIR_ON_MOE}/${LATEST_GZ} ${HOST_CN01}:${DESTDIR_ON_CN01}"
echo "rsync ${DESTDIR_ON_MOE}/${LATEST_GZ} ${HOST_CN01}:${DESTDIR_ON_CN01}" >> ${DESTDIR_ON_MOE}/daily_loading.log
rsync ${DESTDIR_ON_MOE}/${LATEST_GZ} ${HOST_CN01}:${DESTDIR_ON_CN01}

# split files to /data/sdl/public/hadoop-2.5.1-tmp/daily/, this steps takes 90+ mins
ssh ${HOST_CN01} "rm -rf ${DESTDIR_ON_CN01}/input/*"
# module load parallel command outputs as stderr when it's called
ssh ${HOST_CN01} "module load parallel &> /dev/null ; zcat ${DESTDIR_ON_CN01}/${LATEST_GZ} | parallel --will-cite --pipe -j64 --round-robin 'gzip > ${DESTDIR_ON_CN01}/input/${JSON_FILENAME}-{#}.gz'"

# copy gz files from cn01 to HDFS, this steps takes 10+ mins
logger -t truthy-daily-loading "move json file to HDFS daily /truthy/loading/<${YEAR_OF_GZ}-${MONTH_OF_GZ}-${DAY_OF_GZ}> directory"
# hadoop fs -rm -r /truthy/loading/daily/*
hadoop fs -mkdir -p /truthy/loading/${YEAR_OF_GZ}-${MONTH_OF_GZ}-${DAY_OF_GZ} 2>&1
ssh ${HOST_CN01} "hadoop fs -copyFromLocal -f ${DESTDIR_ON_CN01}/input/* /truthy/loading/${YEAR_OF_GZ}-${MONTH_OF_GZ}-${DAY_OF_GZ}/ 2>&1"

# remove tmp files
logger -t truthy-daily-loading "remove tmp files on CN01."
ssh ${HOST_CN01} "rm -rf ${DESTDIR_ON_CN01}/input/*; rm -rf ${DESTDIR_ON_CN01}/${LATEST_GZ};"

ssh-agent -k 1>/dev/null

# asumming the HBase table exists, otherwise, our java program must throws
pushd ${TRUTHY_CMD_DIR} 1>/dev/null

# load-from-hdfs <directory within /truthy/data/>, this steps takes around 1 hour
# hadoop message on MOE outputs as stderr or anything other than stdout, 
# we redirect it back to local file to avoid "annoying" email to
# truthy@indianai.edu, for details, please see crontab -e on node ln01
logger -t truthy-daily-loading "./truthy-cmd.sh load-from-hdfs ${YEAR_OF_GZ}-${MONTH_OF_GZ}-${DAY_OF_GZ} index ${YEAR_OF_GZ}-${MONTH_OF_GZ} ${CUSTOM_INDEX_XML_PATH}"
./truthy-cmd.sh load-from-hdfs ${YEAR_OF_GZ}-${MONTH_OF_GZ}-${DAY_OF_GZ} index ${YEAR_OF_GZ}-${MONTH_OF_GZ} ${CUSTOM_INDEX_XML_PATH} > ${DESTDIR_ON_MOE}/load-from-hdfs.log 2>&1 

if [[ $? != 0 ]]; 
then
   echo "error: truthy-cmd.sh load-from-hdfs exited with non-zero status" 1>&2;
   exit 1;
fi

# remove files under ${TMP_DIR_ON_HDFS}
hadoop fs -rm -f -r /truthy/loading/${YEAR_OF_GZ}-${MONTH_OF_GZ}-${DAY_OF_GZ} >> ${DESTDIR_ON_MOE}/load-from-hdfs.log 2>&1

# remove file under /home/appuser/truthy_tmp directory
rm -rf ${DESTDIR_ON_MOE}/${LATEST_GZ}

logger -t truthy-daily-loading "Finished loading ${LATEST_GZ}."
echo "Finished loading ${LATEST_GZ}." >> ${DESTDIR_ON_MOE}/daily_loading.log

popd 1>/dev/null

# trap - ERR
# trap
