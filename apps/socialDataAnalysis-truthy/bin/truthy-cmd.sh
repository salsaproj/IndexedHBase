#!/bin/bash

usage(){
    echo 'Usage: command <parameters>. Supported commands and parameters:'
    echo '  sort-uid <path to .json.gz file> <path to sorted uid.gz file> (Sort the uid in a .json.gz file for the last day of a month, useful for create-tables)'
    echo '  create-tables <month> [<number of region servers> <path to sorted uid.gz file>]   (Create HBase tables for a given month, e.g. 2010-09)'
    echo '  create-single-table <month> <number of region servers> <table name prefix>   (Create a single HBase table for a given month, e.g. geoIndexTable-2010-09)'
    echo '  delete-tables <month>                    (Delete HBase tables for a given month, e.g. 2010-09)'
    echo '  get-system-status                        (Get a brief summary about HBase system status)'
    echo '  preload <local dir> <dirname on hdfs>    (Load .json.gz files from <local dir> to <dirname on hdfs>)'
    echo '  load-from-hdfs <dirname on hdfs> <index or noindex> <month> [<index config path>]   (Load tweets from .json.gz files in <dirname on hdfs> to HBase tables for <month>, using GeneralCustomIndexer for on-the-fly indexing.)'
    echo '  build-index <index config path> <source data table name> [<index table name>]       (Build indices for data in the source data table.)'
    echo '  read-table <table name> <number of rows to read> [<starting row key>]    (Read a specific number of rows from a table. Optional: starting from the given starting row key.)'
    echo '  get-tweets-with-meme <memes> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  get-tweets-with-text <keywords> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  get-tweets-with-userid <userid> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  get-tweets-with-coordinates <coordinates> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  get-tweets-with-time <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  get-tweets-with-phrase <phrase in double quotes> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  get-uids-with-sname <sname> <start time> <end time> <output directory> [<number of userIds per file>] [<-t or -f as input>]'
    echo '  get-retweets <retweeted tweet id> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  get-tweets-with-mrop <index constraint> <start time> <end time> <tweet-id or tweet-content for output> <output directory> [<number of tweets per ID file>]'
    echo '  timestamp-count <memes> <start time> <end time> <output directory> [<-t or -f as input>]'
    echo '  text-timestamp-count <text> <start time> <end time> <output directory> [<-t or -f as input>]'
    echo '  userid-timestamp-count <userid> <start time> <end time> <output directory> [<-t or -f as input>]'
    echo '  user-post-count <memes> <start time> <end time> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  user-post-count-by-text <text> <start time> <end time> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  meme-post-count <memes> <start time> <end time> <output directory> [<-t or -f as input>]'
    echo '  text-post-count <text> <start time> <end time> <output directory> [<-t or -f as input>]'
    echo '  userid-post-count <userid> <start time> <end time> <output directory> [<-t or -f as input>]'
    echo '  meme-cooccur-count <meme> <start time> <end time> <output directory> [<number of tweets per fiile>] [<-t or -f as input>]'
    echo '  get-retweet-edges <memes> <start time> <end time> <in or out> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  get-mention-edges <memes> <start time> <end time> <in or out> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
    echo '  process-tweet-ids <hdfs input directory> <hdfs output directory> <map class name> <reduce class name> <number of reducer> <compress or nocompress for output> [<additional arguments>]'
    echo '  get-tweets-and-analyze <get-(re)tweets* command> <queried value> <start time> <end time> <map class name> <reduce class name> <compress or nocompress for output> <output directory> <-t or -f as input> [<additional arguments>]'
}

if [ $# -lt 1 ]; then
    usage
    exit -1
fi

IHVER=CoreTruthy-0.2
IHJAR=IndexedHBase-$IHVER.jar
IHJPATH=$HADOOP_HOME/share/hadoop/common/lib/$IHJAR
echo $IHJPATH
case $1 in 'sort-uid')
    if [ $# -lt 3 ]; then
        echo 'Usage: sort-uid <path to .json.gz file> <path to sorted uid.gz file>'
        exit -1
    fi
    JSONGZPATH=$2
    UIDGZPATH=$3
    echo "$HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TableCreatorTruthy sort-uid $JSONGZPATH $UIDGZPATH
"
    sleep 2
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TableCreatorTruthy sort-uid $JSONGZPATH $UIDGZPATH
;;
'create-tables')
    if [ $# -lt 2 ]; then
        echo 'Usage: create-tables <month in the form of yyyy-mm> [<number of region servers> <path to sorted uid.gz file>]'
        exit -1
    fi
    MONTH=$2
    NREG=$3
    UIDPATH=$4
    echo "$HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TableCreatorTruthy create-tables $MONTH $NREG $UIDPATH
"
    sleep 2
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TableCreatorTruthy create-tables $MONTH $NREG $UIDPATH
;;
'create-single-table')
    if [ $# -lt 4 ]; then
        echo 'Usage: create-single-table <month in the form of yyyy-mm> <number of region servers> <table prefix>'
        exit -1
    fi
    MONTH=$2
    NREG=$3
    TABLENAMEPREFIX=$4
    echo "$HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TableCreatorTruthy create-single-table $MONTH $NREG $TABLENAMEPREFIX
"
    sleep 2
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TableCreatorTruthy create-single-table $MONTH $NREG $TABLENAMEPREFIX
;;
'delete-tables')
    if [ $# -lt 2 ]; then
        echo 'Usage: delete-tables <month in the form of yyyy-mm>'
        exit -1
    fi
    MONTH=$2
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TableCreatorTruthy delete-tables $MONTH
;;
'get-system-status')
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TableCreatorTruthy get-system-status
;;
'preload')
    if [ $# -lt 3 ]; then
        echo 'Usage: preload <local dir> <dirname on hdfs>'
        exit -1
    fi
    LOCALDIR=$2
    HDFSDIR=$3
    HDFSPATH=$HDFS_URI/truthy/loading/$HDFSDIR
    echo "making $HDFSPATH ..."
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFSPATH
    echo "copying files from $LOCALDIR to $HDFSPATH ..."
    $HADOOP_HOME/bin/hdfs dfs -copyFromLocal $LOCALDIR/* $HDFSPATH/
    echo "files under $HDFSPATH after preload:"
    $HADOOP_HOME/bin/hdfs dfs -ls $HDFSPATH
;;
'load-from-hdfs')
    if [ $# -lt 4 ]; then
        echo 'Usage: load-from-hdfs <dirname on hdfs> <index or noindex> <month> [<index config path>]'
        exit -1
    fi
    HDFSDIR=$2
    IDXOPT=$3
    MONTH=$4
    CONFPATH=""
    if [ $# -ge 5 ]; then
        CONFPATH=$5
    fi
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyDataLoaderWithGci $HDFS_UIR/truthy/loading/$HDFSDIR $IDXOPT jsoncontent $MONTH $CONFPATH
;;
'build-index')
    if [ $# -lt 3 ]; then
        echo 'Usage: build-index <index config path> <source data table name> [<index table name>]'
        exit -1
    fi
    CONFPATH=$2
    SRCTABLE=$3
    IDXTABLE=""
    if [ $# -ge 4 ]; then
        IDXTABLE=$4
    fi
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.GeneralMRCustomIndexBuilder $CONFPATH $SRCTABLE $IDXTABLE
;;
'read-table')
    if [ $# -lt 3 ]; then
        echo 'Usage: read-table <table name> <number of rows to read> [<starting row key>]'
        exit -1
    fi
    TABLE=$2
    NROWS=$3
    STARTROW=""
    if [ $# -ge 4 ]; then
        STARTROW=$4
    fi
    echo "$HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyTableReader $TABLE $NROWS $STARTROW"
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyTableReader $TABLE $NROWS $STARTROW
;;
'get-tweets-with-meme')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-tweets-with-meme <memes> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    MEMES=$2
    TSTART=$3
    TEND=$4
	IDORALL=$5
	OUTDIR=$6
	FTID=-1
	if [ $# -ge 7 ]; then
		FTID=$7
	fi
        FILE=-t
        if [ $# -ge 8 ]; then
                FILE=$8
        fi
      
    echo $MEMES
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-with-meme $MEMES $TSTART $TEND $IDORALL $FTID $OUTDIR $FILE
;;
'get-tweets-with-phrase')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-tweets-with-phrase <phrase> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    PHRASE=$2
    TSTART=$3
    TEND=$4
	IDORALL=$5
	OUTDIR=$6
	FTID=-1
	if [ $# -ge 7 ]; then
		FTID=$7
	fi
        FILE=-t
        if [ $# -ge 8 ]; then
                FILE=$8
        fi
    echo $PHRASE
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-with-phrase "$PHRASE" $TSTART $TEND $IDORALL $FTID $OUTDIR $FILE
;;
'get-uids-with-sname')
    if [ $# -lt 5 ]; then
        echo 'Usage: get-uids-with-sname <sname> <start time> <end time> <output directory> [<number of uids per file>] [<-t or -f as input>]'
        exit -1
    fi
    SNAME=$2
    TSTART=$3
    TEND=$4
    IDORALL=tweet-id
    OUTDIR=$5
    FTID=-1
    if [ $# -ge 6 ]; then
        FTID=$6
    fi
    FILE=-t
    if [ $# -ge 7 ]; then
        FILE=$7
    fi
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-uids-with-sname $SNAME $TSTART $TEND $IDORALL $FTID $OUTDIR $FILE
;;
'get-tweets-with-time')
    if [ $# -lt 5 ]; then
        echo 'Usage: get-tweets-with-time <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>]'
        exit -1
    fi
    TSTART=$2
    TEND=$3
        IDORALL=$4
        OUTDIR=$5
        FTID=-1
        if [ $# -ge 6 ]; then
                FTID=$6
        fi
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-with-time $TSTART $TEND $IDORALL $FTID $OUTDIR
;;
'get-tweets-with-text')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-tweets-with-text <keywords> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    KEYWORDS=$2
    TSTART=$3
    TEND=$4
    IDORALL=$5
    OUTDIR=$6
    FTID=-1
    if [ $# -ge 7 ]; then
        FTID=$7
    fi
    FILE=-t
    if [ $# -ge 8 ]; then
        FILE=$8
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-with-text $KEYWORDS $TSTART $TEND $IDORALL $FTID $OUTDIR $FILE
;;
'get-tweets-with-userid')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-tweets-with-userid <userid> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    USERID=$2
    TSTART=$3
    TEND=$4
    IDORALL=$5
    OUTDIR=$6
    FTID=-1
    if [ $# -ge 7 ]; then
        FTID=$7
    fi
    FILE=-t
    if [ $# -ge 8 ]; then
        FILE=$8
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-with-userid $USERID $TSTART $TEND $IDORALL $FTID $OUTDIR $FILE
;;
'get-tweets-with-coordinates')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-tweets-with-coordinates <coordinates> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    COORD=$2
    TSTART=$3
    TEND=$4
    IDORALL=$5
    OUTDIR=$6
    FTID=-1
    if [ $# -ge 7 ]; then
        FTID=$7
    fi
    FILE=-t
    if [ $# -ge 8 ]; then
        FILE=$8
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-with-coordinates $COORD $TSTART $TEND $IDORALL $FTID $OUTDIR $FILE
;;
'get-retweets')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-retweets <retweeted tweet id> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    TWEETID=$2
    TSTART=$3
    TEND=$4
    IDORALL=$5
    OUTDIR=$6
    FTID=-1
    if [ $# -ge 7 ]; then
        FTID=$7
    fi
    FILE=-t
    if [ $# -ge 8 ]; then
        FILE=$8
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-retweets $TWEETID $TSTART $TEND $IDORALL $FTID $OUTDIR $FILE
;;
'get-tweets-with-mrop')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-tweets-with-mrop <index constraint> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    IDXCST=$2
    TSTART=$3
    TEND=$4
    IDORALL=$5
    OUTDIR=$6
    FTID=-1
    if [ $# -ge 7 ]; then
        FTID=$7
    fi
    FILE=-t
    if [ $# -ge 8 ]; then
        FILE=$8
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-with-mrop $IDXCST $TSTART $TEND $IDORALL $FTID $OUTDIR $FILE
;;
'timestamp-count')
    if [ $# -lt 5 ]; then
        echo 'Usage: timestamp-count <memes> <start time> <end time> <output directory> [<-t or -f as input>]'
        exit -1
    fi
    MEMES=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FILE=-t
    if [ $# -ge 6 ]; then
        FILE=$6
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver timestamp-count $MEMES $TSTART $TEND $OUTDIR $FILE
;;
'text-timestamp-count')
    if [ $# -lt 5 ]; then
        echo 'Usage: text-timestamp-count <text> <start time> <end time> <output directory> [<-t or -f as input>]'
        exit -1
    fi
    TEXT=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FILE=-t
    if [ $# -ge 6 ]; then
        FILE=$6
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver text-timestamp-count $TEXT $TSTART $TEND $OUTDIR $FILE
;;
'userid-timestamp-count')
    if [ $# -lt 5 ]; then
        echo 'Usage: userid-timestamp-count <userid> <start time> <end time> <output directory> [<-t or -f as input>]'
        exit -1
    fi
    USERID=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FILE=-t
    if [ $# -ge 6 ]; then
        FILE=$6
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver userid-timestamp-count $USERID $TSTART $TEND $OUTDIR $FILE
;;
'user-post-count')
    if [ $# -lt 5 ]; then
        echo 'Usage: user-post-count <memes> <start time> <end time> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    MEMES=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FTID=-1
    if [ $# -ge 6 ]; then
        FTID=$6
    fi
    FILE=-t
    if [ $# -ge 7 ]; then
        FILE=$7
    fi
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver user-post-count $MEMES $TSTART $TEND $FTID $OUTDIR $FILE
;;
'user-post-count-by-text')
    if [ $# -lt 5 ]; then
        echo 'Usage: user-post-count-by-text <text> <start time> <end time> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    TEXT=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FTID=-1
    if [ $# -ge 6 ]; then
        FTID=$6
    fi
    FILE=-t
    if [ $# -ge 7 ]; then
        FILE=$7
    fi
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver user-post-count-by-text $TEXT $TSTART $TEND $FTID $OUTDIR $FILE
;;
'meme-post-count')
    if [ $# -lt 5 ]; then
        echo 'Usage: meme-post-count <memes> <start time> <end time> <output directory> [<-t or -f as input>]'
        exit -1
    fi
    MEMES=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FILE=-t
    if [ $# -ge 6 ]; then
        FILE=$6
    fi
    
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver meme-post-count $MEMES $TSTART $TEND $OUTDIR $FILE
;;
'text-post-count')
    if [ $# -lt 5 ]; then
        echo 'Usage: text-post-count <memes> <start time> <end time> <output directory> [<-t or -f as input>]'
        exit -1
    fi  
    TEXT=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FILE=-t
    if [ $# -ge 6 ]; then
        FILE=$6
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver text-post-count $TEXT $TSTART $TEND $OUTDIR $FILE
;;
'userid-post-count')
    if [ $# -lt 5 ]; then
        echo 'Usage: userid-post-count <userid> <start time> <end time> <output directory> [<-t or -f as input>]'
        exit -1
    fi
    USERID=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FILE=-t
    if [ $# -ge 6 ]; then
        FILE=$6
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver userid-post-count $USERID $TSTART $TEND $OUTDIR $FILE
;;
'meme-cooccur-count')
    if [ $# -lt 5 ]; then
        echo 'Usage: meme-cooccur-count <meme> <start time> <end time> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    MEME=$2
    TSTART=$3
    TEND=$4
    OUTDIR=$5
    FTID=-1
    if [ $# -ge 6 ]; then
        FTID=$6
    fi
    FILE=-t
    if [ $# -ge 7 ]; then
        FILE=$7
    fi
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver meme-cooccur-count $MEME $TSTART $TEND $FTID $OUTDIR $FILE
;;
'get-retweet-edges')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-retweet-edges <memes> <start time> <end time> <in or out> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    MEMES=$2
    TSTART=$3
    TEND=$4
    DIRECTION=$5
    OUTDIR=$6
    FTID=-1
    if [ $# -ge 7 ]; then
        FTID=$7
    fi
    if [ $# -ge 8 ]; then
        FILE=$8
    fi    
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-retweet-edges $MEMES $TSTART $TEND $DIRECTION $FTID $OUTDIR $FILE
;;
'get-mention-edges')
    if [ $# -lt 6 ]; then
        echo 'Usage: get-mention-edges <memes> <start time> <end time> <in or out> <output directory> [<number of tweets per file>] [<-t or -f as input>]'
        exit -1
    fi
    MEMES=$2
    TSTART=$3
    TEND=$4
    DIRECTION=$5
    OUTDIR=$6
    FTID=-1
    if [ $# -ge 7 ]; then
        FTID=$7
    fi
    FILE=-t
    if [ $# -ge 8 ]; then
        FILE=$8
    fi

    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-mention-edges $MEMES $TSTART $TEND $DIRECTION $FTID $OUTDIR $FILE
;;
'process-tweet-ids')
    if [ $# -lt 7 ]; then
        echo 'Usage: process-tweet-id <hdfs input directory> <hdfs output directory> <map class name> <reduce class name> <number of reducers> <compress or nocompress> [<additional arguments>]'
        exit -1
    fi
    INDIR=$2
    OUTDIR=$3
    MAPCLS=$4
    REDCLS=$5
    NRED=$6
    CMPOPT=$7
    OTHER=""
    if [ $# -ge 8 ]; then
        OTHER="${*:8}"
    fi
    echo $OTHER
    $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TweetSubsetProcessor $INDIR $OUTDIR $NRED $MAPCLS $REDCLS org.apache.hadoop.io.Text org.apache.hadoop.io.LongWritable $CMPOPT $OTHER
;;
'get-tweets-and-analyze')
    if [ $# -lt 9 ]; then
        echo 'get-tweets-and-analyze <get-(re)tweets* command> <queried value> <start time> <end time> <map class name> <reduce class name> <compress or nocompress for output> <output directory> <-t for text as queried value / -f for see file as input> [<additional arguments>]'
        exit -1;
    fi
    QUERYCMD=$2
    QUERIEDVAL=$3
    TSTART=$4
    TEND=$5
    MAPCLS=$6
    REDCLS=$7
    CMPOPT=$8
    OUTDIR=$9
    FILE=-t
    if [ $# -ge 10 ]; then
        FILE=${10}
    fi
    OTHER=""
    if [ $# -ge 11 ]; then
        OTHER="${*:11}"
    fi

    echo $OTHER
    if [ $QUERYCMD == "get-tweets-with-phrase" ]; then
        echo "Phrase = $QUERIEDVAL"
        $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-and-analyze $QUERYCMD "$QUERIEDVAL" $TSTART $TEND -1 $MAPCLS $REDCLS org.apache.hadoop.io.Text org.apache.hadoop.io.LongWritable $CMPOPT $OUTDIR $FILE $OTHER
    else
        echo "Queried value = $QUERIEDVAL"
        $HADOOP_HOME/bin/hadoop jar $IHJPATH iu.pti.hbaseapp.truthy.TruthyQueryDriver get-tweets-and-analyze $QUERYCMD $QUERIEDVAL $TSTART $TEND -1 $MAPCLS $REDCLS org.apache.hadoop.io.Text org.apache.hadoop.io.LongWritable $CMPOPT $OUTDIR $FILE $OTHER
    fi 
;;
*)
    usage
    exit -1
;;
esac

