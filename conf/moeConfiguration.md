###Hadoop & HBase Configuration Document for HBase cluster on MOE

@author Stephen Wu
@author Xiaoming Gao

This Document is designed for admin that has access to hadoop user on MOE, we
have merged and updated the first generation of MOE configuration written by
Xiaoming Gao.

####Initial Linux system setup needed to be done by system admins having root access

1. Create /public directory that is writable by normal users. This is used to
   store daemon logs of Hadoop and HBase, as well as zookeeper data.

2. Make the registered DNS hostnames consistent with the 'hostname' command on
   every node. Previously the registered DNS hostnames are like 'moe-cn01d',
   while the 'hostname' command will return 'moe-cn01'; that caused some trouble
   for Hadoop start-up.

3. Make localhost, `127.0.0.1`, and `0.0.0.0` 'ssh'-able without a password.

4. Make sure the `/data/sd*` directories on cn10 are writable for non root accounts.

5. Configure the firewalls on all nodes so that all the nodes within the
   `192.168.101.*` and `10.10.2.*` network can connect to each other through any
   port number.

6. Install ProtocolBuffer 2.5.0, CMake 2.6 or newer, Zlib devel, and openssl
   devel on every node.

   ```   
   Add the following to .bashrc on every node:    
   module load protobuf
   ```           

7. Change the max open files and nproc numbers on every node.

   ```   
   vim /etc/security/limits.conf
   Add "* - nofile 65536" and "* hard nproc 20480"
   vim /etc/sysctl.conf
   Add "fs.file-max = 65536"
   sysctl -p
   vim /etc/security/limits.d/90-nproc.conf
   Change the line "*     soft    nproc     1024" to "*     soft    nproc     20480"
   ```           

####Install Java under shared home directory

1. Make installation directory on every node:

   ```    
    ssh hn
    mkdir -p /home/hadoop/software
   ```        
2. Download and decompress Java:

   ```    
    Download jdk for linux from http://www.oracle.com/technetwork/java/javase/downloads/ to your local machine, and then transfer it to /home/hadoop/software on Moe
    cd /home/hadoop/software
    tar xzvf jdk-7u40-linux-x64.tar.gz
   ```        
3. Add the following to `~/.bashrc`:

   ```    
    export JAVA_HOME=/home/hadoop/software/jdk1.7.0_40
    export PATH=$JAVA_HOME/bin:$PATH
   ```        

####Install Ant under shared home directory

1. Download and decompress Ant:

   ```    
    cd /home/hadoop/software
    wget http://mirror.nexcess.net/apache//ant/binaries/apache-ant-1.9.4-bin.tar.gz
    ls -lath
    tar xzvf apache-ant-1.9.4-bin.tar.gz
   ```    
2. Add the following to `~/.bashrc`:

   ```    
    export ANT_HOME=/home/hadoop/software/apache-ant-1.9.4
    export PATH=$JAVA_HOME/bin:$ANT_HOME/bin:$PATH
   ```     
    
####Install Hadoop under shared home directory:

1. Download and decompress Hadoop 2.5.1:

   ```    
    cd /home/hadoop/software
    wget http://mirror.metrocast.net/apache/hadoop/common/hadoop-2.5.1/hadoop-2.5.1.tar.gz
    tar xzvf hadoop-2.5.1.tar.gz
   ```         

2. Add the following to `~/.bashrc`:

   ```    
    export HADOOP_HOME=/home/hadoop/software/hadoop-2.5.1
    export HADOOP_MAPRED_HOME=$HADOOP_HOME
    export HADOOP_COMMON_HOME=$HADOOP_HOME
    export HADOOP_HDFS_HOME=$HADOOP_HOME
    export YARN_HOME=$HADOOP_HOME
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export HADOOP_PID_DIR=/public/hadoop-2.5.1-pids
    export HADOOP_MAPRED_PID_DIR=$HADOOP_PID_DIR
    export HADOOP_SECURE_DN_PID_DIR=$HADOOP_PID_DIR
    export YARN_PID_DIR=$HADOOP_PID_DIR
    export HADOOP_LOG_DIR=/public/hadoop-logs
    export YARN_LOG_DIR=$HADOOP_LOG_DIR
    export HDFS_URI=hdfs://ln01:44749
    export PATH=$HADOOP_HOME_DIR/bin:$JAVA_HOME/bin:$ANT_HOME/bin:$PATH
   ```       

3. Configure Hadoop 2.5.1

   ```    
    cd $HADOOP_HOME/etc/hadoop/
    vim hosts
        Add lines like '10.10.2.51 ln01' for nodes including ln01 and cn01-cn10

    cd hadoop
    vim core-site.xml and set the following properties:
        hadoop.tmp.dir = /data/sdl/public/hadoop-2.5.1-tmp
        io.seqfile.local.dir = /data/sdl/public/hadoop-2.5.1-tmp/io/local,...,/data/sda/public/hadoop-2.5.1-tmp/io/local
        fs.s3.buffer.dir = /data/sdl/public/hadoop-2.5.1-tmp/s3
        fs.default.name = hdfs://ln01:44749
        io.file.buffer.size = 131072
        fs.inmemory.size.mb = 1024
        io.sort.mb = 512

    vim hdfs-site.xml and set the following properties:
        dfs.namenode.name.dir = file:///public/hadoop-2.5.1-dfs/name
        dfs.namenode.checkpoint.dir = file:///public/hadoop-2.5.1-dfs/namesecondary
        dfs.datanode.data.dir = file:///data/sda/public/hadoop-2.5.1-tmp/dfs/data,...file:///data/sdl/public/hadoop-2.5.1-tmp/dfs/data
        dfs.replication = 3
        dfs.block.size = 67108864
        dfs.datanode.handler.count = 64
        dfs.datanode.max.xcievers = 4096
        dfs.datanode.socket.write.timeout = 0
        dfs.datanode.address = 0.0.0.0:44849
        dfs.datanode.ipc.address = 0.0.0.0:44859
        dfs.datanode.http.address = 0.0.0.0:44869
        dfs.permissions = false
        dfs.support.append = true

    vim mapred-site.xml and set the following properties:
        mapreduce.cluster.local.dir = file:///data/sdl/public/hadoop-2.5.1-tmp/mapred/local,...,file:///data/sda/public/hadoop-2.5.1-tmp/mapred/local
        mapreduce.jobtracker.system.dir = file:///data/sdk/public/hadoop-2.5.1-tmp/mapred/system
        mapreduce.jobtracker.staging.root.dir = file:///data/sdk/public/hadoop-2.5.1-tmp/mapred/staging
        mapreduce.cluster.temp.dir = file:///data/sdk/public/hadoop-2.5.1-tmp/mapred/temp
        mapreduce.framework.name = yarn
        mapreduce.jobtracker.address = 0.0.0.0:44750
        mapreduce.jobhistory.address = 0.0.0.0:0
        mapreduce.tasktracker.http.address = 0.0.0.0:0
        mapreduce.jobtracker.http.address = 0.0.0.0:0
        mapreduce.shuffle.port = 43562
        mapreduce.reduce.shuffle.parallelcopies = 10
        mapreduce.tasktracker.map.tasks.maximum = 20
        mapreduce.tasktracker.reduce.tasks.maximum = 8
        mapreduce.job.jvm.numtasks = -1
        mapreduce.map.memory.mb = 2500
        mapreduce.map.java.opts = -Xmx2048m
        mapreduce.reduce.memory.mb = 2500
        mapreduce.reduce.java.opts = -Xmx2048m
        mapred.task.timeout = 1200000
        mapreduce.jobtracker.taskscheduler = org.apache.hadoop.mapred.LocalityFirstTaskScheduler

    vim yarn-site.xml and set the following properties:
        mapreduce.jobhistory.recovery.store.fs.uri = file:///public/hadoop-2.5.1-mrHist/recoverystore
        yarn.nodemanager.aux-services = mapreduce_shuffle
        yarn.nodemanager.aux-services.mapreduce.shuffle.class = org.apache.hadoop.mapred.ShuffleHandler
        yarn.resourcemanager.resource-tracker.address = ln01:48025
        yarn.resourcemanager.scheduler.address = ln01:48030
        yarn.resourcemanager.address = ln01:48040
        yarn.nodemanager.address = 0.0.0.0:0
        yarn.nodemanager.localizer.address = 0.0.0.0:0
        yarn.nodemanager.webapp.address = 0.0.0.0:0
        yarn.application.classpath = $HADOOP_CLASSPATH
        yarn.nodemanager.pmem-check-enabled = false
        yarn.nodemanager.resource.memory-mb = 75000
        yarn.scheduler.minimum-allocation-mb = 2600
        yarn.log-aggregation-enable = true
        yarn.nodemanager.remote-app-log-dir = /logs

    vim hadoop-env.sh and set the following values:
        export HADOOP_HEAPSIZE=3072
        export HADOOP_CLIENT_OPTS="-Xmx2048m $HADOOP_CLIENT_OPTS"
        
    vim mapred-env.sh and set the following values:
        export HADOOP_JOB_HISTORYSERVER_HEAPSIZE=2048

    vim slaves
        Add cn01 - cn10

    cp /home/hadoop/software/IndexedHBase-Truthy-lib/*.jar $HADOOP_HOME/share/hadoop/common/lib/
   ```    

4. Solve the issue of "Unable to load native-hadoop library for your platform" warning when executing hadoop commands:

   ```    
    cd ~/software
    cp hadoopLibNative64bit/*.so.* hadoop-2.5.1/lib/native/
    ls -lath hadoop-2.5.1/lib/native/
   ```         
    
####Install HBase 0.94.23 under shared home directory

1. Download Maven and HBase 0.94.23:

   ```    
    cd ~/software/
    wget http://www.interior-dsgn.com/apache/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz
    tar xzvf apache-maven-3.2.3-bin.tar.gz
    wget http://mirror.tcpdiag.net/apache/hbase/hbase-0.94.23/hbase-0.94.23.tar.gz
    tar xzvf hbase-0.94.23.tar.gz
    vim ~/.bashrc
        export MAVEN_HOME=/home/hadoop/software/apache-maven-3.2.3
        export PATH=$MAVEN_HOME/bin:$PATH
   ```      

2. Compile HBase against Hadoop 2.5.1:

   ```    
    cd ~/software/hbase-0.94.23
    vim pom.xml
        <protobuf.version>2.5.0</protobuf.version>
        <hadoop.version>2.5.1</hadoop.version> (under profile with <id>hadoop-2.0</id>)
        <slf4j.version>1.7.5</slf4j.version> (under profile with <id>hadoop-2.0</id>)
    cd src/main/protobuf/
    protoc -I./ --java_out=/home/hadoop/software/hbase-0.94.23/src/main/java ErrorHandling.proto
    protoc -I./ --java_out=/home/hadoop/software/hbase-0.94.23/src/main/java hbase.proto
    cd ~/software/hbase-0.94.23
    mvn clean install assembly:single -Dhadoop.profile=2.0 -DskipTests
   ```    

3. Move compiled HBase 0.94.23 to `~/software/`

   ```    
    cd ~/software/
    mv hbase-0.94.23 hbase-0.94.23-src
    mv hbase-0.94.23.tar.gz hbase-0.94.23-src.tar.gz
    cd hbase-0.94.23-src
    mv target/hbase-0.94.23/hbase-0.94.23/ ~/software/
   ```       

4. Add the following to `~/.bashrc`

   ```    
    export HBASE_HOME=/home/hadoop/software/hbase-0.94.23
    export HBASE_CLASSPATH=`$HBASE_HOME/bin/hbase classpath`
    export HADOOP_CLASSPATH=$HBASE_CLASSPATH
    export HBASE_PID_DIR=$HADOOP_PID_DIR
    export PATH=$HBASE_HOME/bin:$PATH
   ```       
    
5. Configure HBase 0.94.23

   ```    
    cd ~/software//hbase-0.94.23/conf
    chmod +x hbase-env.sh

    vim hbase-env.sh and set the following variables:
        export JAVA_HOME=/home/hadoop/software/jdk1.7.0_40
        export HBASE_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
        export HBASE_HEAPSIZE=40960
        export HBASE_OPTS="$HBASE_OPTS -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=65"
        export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS $HBASE_JMX_BASE -Xmx3072m -Dcom.sun.management.jmxremote.port=10101"
        export HBASE_ZOOKEEPER_OPTS="$HBASE_ZOOKEEPER_OPTS $HBASE_JMX_BASE -Xmx2048m -Dcom.sun.management.jmxremote.port=10104"
        export HBASE_LOG_DIR=/public/hbase-0.94.23-logs

    vim regionservers
        Add moe-cn01 to moe-cn10

    vim hbase-site.xml and set the following properties:
        hbase.rootdir = hdfs://ln01:44749/hbase
        hbase.cluster.distributed = true
        hbase.zookeeper.property.clientPort = 45749
        hbase.zookeeper.leaderport = 45788
        hbase.zookeeper.quorum = moe-hn,moe-ln01,moe-ln02
        hbase.zookeeper.property.dataDir = /public/zooKeeperData
        hbase.hregion.memstore.block.multiplier = 8
        hbase.hstore.blockingStoreFiles = 1000
        hbase.hregion.memstore.flush.size = 335544320
        hbase.hregion.majorcompaction = 2592000000
        hbase.hregion.memstore.mslab.enabled = true
        hbase.regionserver.global.memstore.upperLimit = 0.45
        hbase.regionserver.global.memstore.lowerLimit = 0.4
        hfile.block.cache.size = 0.3
        hbase.hstore.blockingWaitTime = 50000
        hbase.rpc.timeout = 90000
        zookeeper.session.timeout = 90000

    vim log4j.properties in $HADOOP_HOME/etc/hadoop and set the following properties:
        log4j.logger.org.apache.hadoop.hbase=INFO
   ```    

6. Solve the issue of `Unable to load native-hadoop library for your platform` warning when executing hadoop commands

   ```      
    cd ~/software
    cp hadoopLibNative64bit/*.so.* hbase-0.94.23/lib/native/
    ls -lath hbase-0.94.23/lib/native/
   ```    
    
####Important additional information about HBase configuration

1. Here are the current region sizes used for different tables when they are
   created. They may need to be changed based on the grow data size for each
   month (in the source code of `TableCreatorTruthy.java`).

   ```  
    memeIndexTable: 10GB
    retweetIndexTable: 10GB
    snameIndexTable: 10GB
    textIndexTable: 15GB
    tweetTable: 25GB
    userTable: 25GB
    userTweetsIndexTable: 10GB
   ```      

2. An important blog post from Lars Hofhansl about "HBase region server memory sizing"
    http://hadoop-hbase.blogspot.com/2013/01/hbase-region-server-memory-sizing.html

3. Here is the major bug of HBase with versions >= 0.96.0 on JIRA, which forces
   us to stick to HBase 0.94.*:
    https://issues.apache.org/jira/browse/HBASE-10499
    
####Start Hadoop and HBase:

   ```    
    ssh ln01
    hdfs namenode -format (Be careful when doing this. You don't want to erase data from a production deployment)
    start-dfs.sh
    hdfs dfsadmin -report | head -n 12 
    start-yarn.sh

    ssh ln02
    start-hbase.sh
    hbase shell
        >status
        >list

# or use the start script
    start-hadoop-hbase.sh        
   ```    

####Stop HBase and Hadoop:

```   
    ssh ln02
    stop-hbase.sh
    
    ssh ln01
    stop-yarn.sh
    stop-dfs.sh 
# or use the stop script
    stop-hadoop-hbase.sh       
   ```    

####Install storm and ActiveMQ under shared home directory (Optional)

1. Copy apache-activemq-5.4.2-bin.tar.gz from madrid, and put it under `~/software`

   ```      
    tar xzvf apache-activemq-5.4.2-bin.tar.gz
   ```    

2. Add the following to `~/.bashrc`:

   ```    
    export ACTIVEMQ_HOME=/home/hadoop/software/apache-activemq-5.4.2
    export PATH=$ACTIVEMQ_HOME/bin:$PATH
   ```       

3. Clone supun's storm installation package from github:

   ```    
    ssh ln02
    cd software
    git clone https://github.com/iotcloud/storm_cluster.git
   ```      
    
4. Install and configure storm:

   ```    
    cd storm_cluster/
    vim storm.yaml and set the following configurations:
        storm.zookeeper.servers: "ln02"
        nimbus.host: "ln02"
        supervisor.slots.ports: 6700
        worker.childopts: "-Xmx14336m"
        storm.local.dir: "/public/storm-local"
    ./install.sh
    ls
    
    cp ../apache-activemq-5.4.2/activemq-all-5.4.2.jar storm/lib/
    cp ../IndexedHBase-Truthy-lib/dom4j-1.5.jar storm/lib/
    cp ../IndexedHBase-Truthy-lib/gson-2.2.2.jar storm/lib/
    cp ../hbase-0.94.23/lib/hadoop-*.jar storm/lib/
    cp ../hbase-0.94.23/hbase-0.94.23.jar storm/lib/
    cp ../IndexedHBase-Truthy-lib/lucene-core-3.6.2.jar storm/lib/
    cp ../IndexedHBase-Truthy-lib/nekohtml.jar storm/lib/
    cp ../IndexedHBase-Truthy-lib/xercesMinimal.jar storm/lib/

    vim storm/logback/cluster.xml and change the following configurations:
        appender name="A1": 
            <file>/public/storm-local/logs/${logfile.name}</file>
            <fileNamePattern>/public/storm-local/logs/${logfile.name}.%i</fileNamePattern>        
   ```    

5. Start and stop Storm:

   ```    
    vim run.sh
        change 'm1' to 'ln02'
        change 'storm_cluster' directory to ~/software/storm_cluster
        replace 'm2',... with 'cn01',...        

    vim kill.sh
        replace 'm2',... with 'cn01',...
        change 'm1' to 'ln02'
        change 'storm_cluster' directory to ~/software/storm_cluster
   ```           
