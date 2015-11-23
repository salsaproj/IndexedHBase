#!/bin/bash

DESTDIR_ON_MOE=/home/projects/truthy_tmp
TIMEOUT_IN_SECOND=14400
USER=appuser

isRunningTooLong() 
{
    # ${1} = JOB_START_TIME IN SECONDS, ${2} = CURRENT TIME IN SECONDS
    # cal the start time diff in seconds
    TIME_DIFF=$((${2} - ${1}))
    if [[ "${TIME_DIFF}" -ge "${TIMEOUT_IN_SECOND}" ]] ; then 
        return 0;
    fi 
    return 1;
}

source ${HOME}/.bash_profile

# check get the job information from hadoop command
JOB_INFO_LINES=`hadoop job -list 2> /dev/null`

# check each job
lineCount=1
while read -r line; do  
  # running job information starts from line 3
  if [[ "${lineCount}" -gt 2 ]]; then 
      JOB_INFO_ARRAY=(${line})
      JOB_ID=${JOB_INFO_ARRAY[0]}
      JOB_STATUS=${JOB_INFO_ARRAY[1]}
      JOB_START_TIME=$((${JOB_INFO_ARRAY[2]} / 1000))
      JOB_USERID=${JOB_INFO_ARRAY[3]}
      CURRENT_TIME_IN_SECOND=`date +%s`
      
      # check if job is running too long
      if isRunningTooLong ${JOB_START_TIME} ${CURRENT_TIME_IN_SECOND} ; then 
          logger -t hadoop-long-job-killer "$JOB_ID is running longer than ${TIMEOUT_IN_SECOND} seconds...."
          # check if this is general query
          if [ "${JOB_USERID}" == "$USER" ]; then 
              logger -t hadoop-long-job-killer "hadoop job -kill $JOB_ID"
              hadoop job -kill $JOB_ID 2> /dev/null
              
              if [[ $? != 0 ]]; then
                   echo "error: Hadoop job $JOB_ID cannot be killed. " 1>&2;
                   exit 1;
              fi
          else 
              # send a remind email to developer on MOE
              echo "If you do not expect this long running job $JOB_ID, Please login to MOE and terminate it." \
              | mail -s "[MOE@IU] Hadoop job $JOB_ID started at `date -d @${JOB_START_TIME}` on MOE is running longer than ${TIMEOUT_IN_SECOND} seconds" \
              ${JOB_USERID}@indiana.edu   
          fi             
      fi
  fi
  lineCount=$((${lineCount}+1))
done <<< "${JOB_INFO_LINES}"
