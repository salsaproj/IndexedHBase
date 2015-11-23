## Getting started with the IndexedHBase API for Truthy
This document covers a brief guideline for using the command-line tool of truthy-cmd.sh and extending it for additional Tweets' analyses by adding a MapReduce Mapper and using IndexedHBase's get-tweets-and-analyze interface [1-3].

## Summary of Instructions
* Environment setup
  * Java
  * Apache Ant
  * A copy of /home/hadoop/.bashrc to your home directory, modify it if necessary.
  * Compile IndexedHBase-0.94 and rename the compiled jar file
    * remove Apache Storm streaming code
* Run it with truthy-cmd.sh 
  
The following instructions are designed for users with direct Linux login access to MOE cluster.

## Using IndexedHBase-0.94
1. Make sure you have the environment setup by looking at /home/hadoop/.bashrc, and setup your own ~/.bashrc 

2. Download, unzip, and compile IndexedHBase-0.94
  * The github link is following, and we are using hbase-0.94 branch in MOE. (ignore 0.96 as it's not using on MOE cluster at this moment)
https://github.iu.edu/truthy-team/IndexedHBase
  

  Assuming you have downloaded or checked out a copy to local path such as /home/taklwu/IndexedHBase-hbase0.94, you will first need to ensure that Apache Ant and Java binary are setup correctly in your environment path. Then you can use ```ant``` command to compile it. The compiled jar IndexedHBase-All-0.2.jar will be located in dist/lib directory.

  ```
  [taklwu@moe-ln01 IndexedHBase-hbase0.94]$ ant
  ```
  
  P.S. Compiler may show errors. One of the causes could be the existence of streaming API. To fix this error, you may need to remove the directory of the streaming API and also fix a few Java files if they import any related streaming class. The directory corresponding to streaming API is apps/socialDataAnalysis-truthy/src/iu/pti/hbaseapp/truthy/streaming/.

  After the first compile without editing any code, you are ready to write your own code, and please remember to add your new code under the directory apps/socialDataAnalysis-truthy/src/iu/pti/hbaseapp/truthy/.

  At the end, copy and rename your compiled IndexedHBase-All-0.2.jar to $HADOOP_HOME/share/hadoop/common/lib/, e.g. $HADOOP_HOME/share/hadoop/common/lib/IndexedHBase-taklwu.jar
  ```
  [taklwu@moe-ln01 IndexedHBase-hbase0.94]$ cp dist/lib/IndexedHBase-All-0.2.jar $HADOOP_HOME/share/hadoop/common/lib/IndexedHBase-taklwu.jar
  ```

3. Run it with truthy-cmd.sh

  Reuse get-tweets-with-X and passing queried value, e.g. hashtags, and use your own mapper class and reducer class (if any, otherwise -nored). The following is the example of using iu.pti.hbaseapp.truthy.mrqueries.MemeCooccurCountMapper with no reducer

  ```
  ./truthy-cmd.sh get-tweets-and-analyze get-tweets-with-meme "#ff,#NBA" 2012-11-01 2012-11-02 iu.pti.hbaseapp.truthy.mrqueries.MemeCooccurCountMapper -nored nocompress 20150402
  ```

4. Run get-tweets-and-analyze with GetTweetsMapper

  We have added output filtering for hiding json object from the public web API, get-tweets-and-analyze with extra arguments "--json-include" or "--json-exclude" along with fields split by comma (no space in between) (tweet, user, and retweeted_status [4]) mentioned in the Full Json Object.

  ```
  #case for --json-include
  ./truthy-cmd.sh get-tweets-and-analyze get-tweets-with-meme "#ff,#NBA" 2013-01-01 2013-01-01 iu.pti.hbaseapp.truthy.mrqueries.GetTweetsMapper -nored nocompress includeTextUserCreateTimeDescriptionRetweetText -t --json-include user.description,text,user.created_at,retweeted_status.text
  
  #case for --json-exclude
  ./truthy-cmd.sh get-tweets-and-analyze get-tweets-with-meme "#ff,#NBA" 2013-01-01 2013-01-01 iu.pti.hbaseapp.truthy.mrqueries.GetTweetsMapper -nored nocompress excludeRetweet2 -t --json-exclude created_at,user.following,user.lang,user.utc_offset,retweeted_status.created_at,retweeted_status.user.lang,retweeted_status.user.utc_offset
  ```
  
5. Run get-tweets-with-X with file as input

  "file as input" function can be added to all get-tweets-with-X by passing a local file path to the truthy-cmd.sh; you must give two parameters [<number of tweets per file>] and [<-t or -f as input>] to the existing command-line queries.
  
  ```
  # usage examples
  ./truthy-cmd.sh get-tweets-with-meme memes.txt 2014-01-02 2014-01-30 tweet-id testMeme 100000 -f
  ./truthy-cmd.sh get-tweets-with-text ~/text.txt 2014-01-02 2014-01-03 tweet-id testText 100000 -f
  ./truthy-cmd.sh get-tweets-with-userid ~/test_stephen.txt 2014-01-02 2014-01-10 tweet-id testUserID 100000 -f
  ./truthy-cmd.sh get-tweets-and-analyze <get-(re)tweets* command> <queried value> <start time> <end time> <map class name> <reduce class name> <compress or nocompress for output> <output directory> <-t or -f as input> [<additional arguments>]
  ```
  


##Reference
1. IndexedHBase home page, http://salsaproj.indiana.edu/IndexedHBase
2. Truthy analyze with IndexedHBase, http://salsaproj.indiana.edu/IndexedHBase/truthy.html 
3. Related Publications, http://salsaproj.indiana.edu/IndexedHBase/publications.html
4. Fields can be filtered by GetTweetsMapper, https://github.iu.edu/truthy-team/IndexedHBase/blob/hbase0.94/apps/socialDataAnalysis-truthy/src/iu/pti/hbaseapp/truthy/TweetData.java
