# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# User specific aliases and functions
export JAVA_HOME=/home/hadoop/software/jdk1.7.0_40
export ANT_HOME=/home/hadoop/software/apache-ant-1.9.4
export HADOOP_HOME=/home/hadoop/software/hadoop-2.5.1
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HBASE_HOME=/home/hadoop/software/hbase-0.94.23
export HADOOP_PID_DIR=/public/hadoop-2.5.1-pids
export HADOOP_MAPRED_PID_DIR=$HADOOP_PID_DIR
export HADOOP_SECURE_DN_PID_DIR=$HADOOP_PID_DIR
export YARN_PID_DIR=$HADOOP_PID_DIR
export HADOOP_LOG_DIR=/public/hadoop-logs
export YARN_LOG_DIR=$HADOOP_LOG_DIR
export HBASE_PID_DIR=$HADOOP_PID_DIR
export HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath`
export HDFS_URI=hdfs://ln01:44749
export HADOOP_LIBEXEC_DIR=$HADOOP_HOME/libexec/

# Pig environment
export PIG_HOME=/home/hadoop/software/pig-0.14.0
export HIVE_HOME=/home/hadoop/software/apache-hive-0.13.1

export MAVEN_HOME=/home/hadoop/software/apache-maven-3.2.3
export PROTOC_PATH=/home/hadoop/software/protobuf-2.6.0/build/bin/protoc
export LD_LIBRARY_PATH=$PROTOC_PATH/lib:$LD_LIBRARY_PATH

export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export PATH=$PIG_HOME/bin:$PROTOC_PATH/bin:$MAVEN_HOME/bin:$HBASE_HOME/bin:$ANT_HOME/bin:$PATH

export SPARK_HOME=/home/hadoop/software/spark-1.3.0-hadoop2.4-plus
export SPARK_CLASSPATH=/home/hadoop/software/spark-1.3.0-hadoop2.4-plus/lib/*:$HIVE_HOME/lib/*:$HIVE_HOME/conf/hive-site.xml:/home/hadoop/software/spark-1.3.0-hadoop2.4-plus/bin/hive13-with-indexedhbase94.jar:/home/hadoop/software/spark-1.3.0-hadoop2.4-plus/lib/gson-2.2.2.jar:/home/hadoop/software/spark-1.3.0-hadoop2.4-plus/lib/hive-exec-0.13.1a.jar:/home/hadoop/software/spark-1.3.0-hadoop2.4-plus/lib/kryo-2.21.jar:/home/hadoop/software/apache-hive-0.13.1/lib/*:$HBASE_HOME/hbase-0.94.23.jar:/home/hadoop/software/IndexedHBase-CoreTruthy-0.2/lib/IndexedHBase-CoreTruthy-0.2.jar:/home/hadoop/software/hbase-0.94.23/conf/hbase-site.xml:$SPARK_CLASSPATH
export SPARK_SUBMIT_CLASSPATH=$SPARK_CLASSPATH:$SPARK_SUBMIT_CLASSPATH:.
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
export SPARK_OPTS="-Djava.library.path=$SPARK_HOME/lib"
