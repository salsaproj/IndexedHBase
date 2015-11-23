###Hadoop and HBase configuration  
           
* env: user/developer environment settings in .bashrc
* hadoop-2.5.1 : Resource management for Hadoop (IndexedHBase) jobs running on MOE, HDFS and MapReduce configuration
* hbase-0.94.23 : HBase configuration
* Start and Stop all scripts: start-hadoop-hbase.sh and stop-hadoop-hbase.sh

####General Working Environment Setup for User and Developer

Each user and developer on MOE have to setup the following environment
parameters in `~/.bashrc` correctly in order to execute any IndexedHBase's
truthy-cmd.sh related queries.

```   
export JAVA_HOME={your ant installation directory}
export ANT_HOME={your ant installation directory}
export HADOOP_HOME={your hadoop installation directory}
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HBASE_HOME={your hbase installation directory}
export HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath
export HDFS_URI=hdfs://{your hdfs name node hostname}:{your hdfs name node port number}
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
export PATH=$HBASE_HOME/bin:$ANT_HOME/bin:$PATH
```   

Admin (hadoop user on MOE) needs to add more parameters to the working
environment, please see the example in `env/.bashrc`

####Hadoop 2.5.1 Configration for Admin 
For Hadoop, edit the following files under directory
`${HADOOP_HOME}/etc/hadoop`, please see [moeConfiguration.md](moeConfiguration.md) for details

* yarn-site.xml
* hdfs-site.xml
* mapreduce-site.xml
* core-site.xml
* slaves
* log4j.properties (keep one copy in $HADOOP_HOME/etc/hadoop for both Hadoop and HBase)

####HBase 0.94.23 Configuration for Admin
For HBase, edit the following files under directory `${HBASE_HOME}/conf`, please see
[moeConfiguration.md](moeConfiguration.md) for details HBase loads the same log4j.properties mentioned
in Hadoop, if you need to override the logging setup, please edit the
log4j.properties located at `${HADOOP_HOME}/etc/hadoop`.

* hbase-site.xml
* hbase-env.sh
* regionservers





