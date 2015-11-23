####IndexedHBase API usage for Truthy data
* truthy-cmd.sh - main execution scipt calls the java libraries, usage shown in the below sections.
* daily_loading_moe.sh - a bash script runs daily as cron job on MOE-LN01 for automatic loading and indexing to HBase by calling truthy-cmd.sh on MOE ln01 login node.
* moe_copy_hdfs.sh - a bash script copy the daily corrected twitter data from SMITHERS@CS machine to MOE.
* long_jobs_on_yarn_killer.sh - a bash script runs daily as cron job on MOE-LN01 for terminating long running Hadoop jobs on MOE.  
* hdfs_metadata_backup.sh - a bash script runs daily as cron job on MOE-LN01 with user as hadoop for backup the hdfs namenode metadata, we keep the most recent 3 days metadata on disk under /home/projects/yarn/backup/hdfs/. 

#####Truthy Queries:
<pre>
./bin/truthy-cmd.sh get-tweets-with-meme "#p2,#tcot" 2014-08-01 2014-08-31 tweet-content /home/gao4/truthy-data/results/getTweetsP2Tcot 10000

./bin/truthy-cmd.sh meme-cooccur-count "#p2" 2014-08-01 2014-08-31 /home/gao4/truthy-data/results/memeCooccurP2 10000

./bin/truthy-cmd.sh get-tweets-with-text yolo 2014-08-01 2014-08-31 tweet-content /home/gao4/truthy-data/results/getTweetsYolo 10000

./bin/truthy-cmd.sh get-tweets-with-userid 2544475070 2014-08-01 2014-08-31 tweet-id /home/gao4/truthy-data/results/getTweetsUid2544475070 2000

./bin/truthy-cmd.sh get-retweets 505165206058975233 2014-08-01 2014-08-31 tweet-id /home/gao4/truthy-data/results/getRetweets505165206058975233 2000

./bin/truthy-cmd.sh timestamp-count "#p2,#tcot" 2014-08-01 2014-08-31 /home/gao4/truthy-data/results/tscP2Tcot

./bin/truthy-cmd.sh user-post-count "#p2,#tcot" 2014-08-01 2014-08-31 /home/gao4/truthy-data/results/upcP2Tcot 10000

./bin/truthy-cmd.sh meme-post-count "#p2,#tcot" 2014-08-01 2014-08-31 /home/gao4/truthy-data/results/mpcP2Tcot

./bin/truthy-cmd.sh get-retweet-edges "#p2,#tcot" 2014-08-01 2014-08-31 in /home/gao4/truthy-data/results/greP2Tcot 10000

./bin/truthy-cmd.sh get-mention-edges "#p2,#tcot" 2014-08-01 2014-08-31 in /home/gao4/truthy-data/results/gmeP2Tcot 10000

# use file as a input to truthy-cmd.sh
./bin/truthy-cmd.sh get-tweets-with-meme memes.txt 2014-01-02 2014-01-30 tweet-id testMeme 100000 -f

./bin/truthy-cmd.sh get-tweets-with-text ~/text.txt 2014-01-02 2014-01-03 tweet-id testText 100000 -f

./bin/truthy-cmd.sh get-tweets-with-userid ~/test_stephen.txt 2014-01-02 2014-01-10 tweet-id testUserID 100000 -f

</pre>

#####Queries by MapReduce:
<pre>
./bin/truthy-cmd.sh get-tweets-with-meme "#p2,#tcot" 2014-08-01 2014-08-31 tweet-id hdfs://ln01:44749/truthy/queries/2014-10/retweetEdgeListP2Tcot 10000

./bin/truthy-cmd.sh process-tweet-ids hdfs://ln01:44749/truthy/queries/2014-10/retweetEdgeListP2Tcot/tweetIds hdfs://ln01:44749/truthy/queries/2014-10/retweetEdgeListP2Tcot/mrOutput iu.pti.hbaseapp.truthy.mrqueries.RetweetEdgeListMapper iu.pti.hbaseapp.truthy.mrqueries.TextCountReducer 3 nocompress

hdfs dfs -copyToLocal hdfs://ln01:44749/truthy/queries/2014-10/retweetEdgeListP2Tcot /home/gao4/truthy-data/results/
</pre>

#####Related hashtag mining:
<pre>
./bin/truthy-cmd.sh get-tweets-with-meme "#tcot" 2014-08-01 2014-08-31 tweet-id hdfs://ln01:44749/truthy/queries/2014-10/relatedHashtagTcot 10000

./bin/truthy-cmd.sh process-tweet-ids hdfs://ln01:44749/truthy/queries/2014-10/relatedHashtagTcot/tweetIds hdfs://ln01:44749/truthy/queries/2014-10/relatedHashtagTcot/mrOutput iu.pti.hbaseapp.truthy.mrqueries.MemeCooccurCountMapper iu.pti.hbaseapp.truthy.mrqueries.RelatedHashtagReducer 3 nocompress "#tcot" 0.005 2014-08-01 2014-08-31

hdfs dfs -copyToLocal hdfs://ln01:44749/truthy/queries/2014-10/relatedHashtagTcot /home/gao4/truthy-data/results/

#Or simply use get-tweets-and-analyze
./bin/truthy-cmd.sh get-tweets-and-analyze [get-(re)tweets* command] [queried value] [start time] [end time] [map class name] [reduce class name] [compress or nocompress for output] [output directory] [-t or -f as input] [<additional arguments>]
</pre>

