#!/usr/bin/env bash

###############################################################################
# Script that is run on each EC2 instance on boot. It is passed in the EC2 user
# data, so should not exceed 16K in size.
###############################################################################

MASTER_HOST=%MASTER_HOST%
ZOOKEEPER_QUORUM=%ZOOKEEPER_QUORUM%
SECURITY_GROUPS=`wget -q -O - http://169.254.169.254/latest/meta-data/security-groups`
IS_MASTER=`echo $SECURITY_GROUPS | awk '{ a = match ($0, "-master$"); if (a) print "true"; else print "false"; }'`
if [ "$IS_MASTER" = "true" ]; then
 MASTER_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/local-hostname`
fi
HADOOP_HOME=`ls -d /usr/local/hadoop-*`
HADOOP_VERSION=`echo $HADOOP_HOME | cut -d '-' -f 2`
HBASE_HOME=`ls -d /usr/local/hbase-*`
HBASE_VERSION=`echo $HBASE_HOME | cut -d '-' -f 2`

###############################################################################
# Hadoop configuration
###############################################################################

cat > $HADOOP_HOME/conf/core-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/hadoop</value>
</property>
<property>
  <name>fs.default.name</name>
  <value>hdfs://$MASTER_HOST:8020</value>
</property>
</configuration>
EOF

cat > $HADOOP_HOME/conf/hdfs-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>fs.default.name</name>
  <value>hdfs://$MASTER_HOST:8020</value>
</property>
</configuration>
EOF

cat > $HADOOP_HOME/conf/mapred-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>mapred.job.tracker</name>
  <value>$MASTER_HOST:8021</value>
</property>
<property>
  <name>io.compression.codecs</name>
  <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec</value>
</property>
<property>
  <name>io.compression.codec.lzo.class</name>
  <value>com.hadoop.compression.lzo.LzoCodec</value>
</property>
<property>
  <name>mapred.map.output.compression.codec</name>
  <value>com.hadoop.compression.lzo.LzoCodec</value>
</property>
</configuration>
EOF

# Update classpath to include HBase jars and config
cat >> $HADOOP_HOME/conf/hadoop-env.sh <<EOF
HADOOP_CLASSPATH="$HBASE_HOME/hbase-${HBASE_VERSION}.jar:$HBASE_HOME/lib/AgileJSON-2009-03-30.jar:$HBASE_HOME/lib/json.jar:$HBASE_HOME/lib/zookeeper-3.2.1.jar:$HBASE_HOME/conf"
EOF

# Configure Hadoop for Ganglia
# overwrite hadoop-metrics.properties
cat > $HADOOP_HOME/conf/hadoop-metrics.properties <<EOF
dfs.class=org.apache.hadoop.metrics.ganglia.GangliaContext
dfs.period=10
dfs.servers=$MASTER_HOST:8649
jvm.class=org.apache.hadoop.metrics.ganglia.GangliaContext
jvm.period=10
jvm.servers=$MASTER_HOST:8649
mapred.class=org.apache.hadoop.metrics.ganglia.GangliaContext
mapred.period=10
mapred.servers=$MASTER_HOST:8649
EOF

###############################################################################
# HBase configuration
###############################################################################

cat > $HBASE_HOME/conf/hbase-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>hbase.rootdir</name>
  <value>hdfs://$MASTER_HOST:8020/hbase</value>
</property>
<property>
  <name>hbase.cluster.distributed</name>
  <value>true</value>
</property>
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>$ZOOKEEPER_QUORUM</value>
</property>
<property>
  <name>hbase.regionserver.handler.count</name>
  <value>100</value>
</property>
<property>
  <name>hbase.hregion.memstore.block.multiplier</name>
  <value>3</value>
</property>
<property>
  <name>hbase.hstore.blockingStoreFiles</name>
  <value>15</value>
</property>
<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>
<property>
  <name>dfs.client.block.write.retries</name>
  <value>100</value>
</property>
<property>
  <name>zookeeper.session.timeout</name>
  <value>60000</value>
</property>
<property>
  <name>hbase.tmp.dir</name>
  <value>/mnt/hbase</value>
</property>
</configuration>
EOF

# Override JVM options
cat >> $HBASE_HOME/conf/hbase-env.sh <<EOF
export HBASE_MASTER_OPTS="-XX:+UseConcMarkSweepGC -XX:+DoEscapeAnalysis -XX:+AggressiveOpts -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/mnt/hbase/logs/hbase-master-gc.log"
export HBASE_REGIONSERVER_OPTS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=88 -XX:+DoEscapeAnalysis -XX:+AggressiveOpts -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/mnt/hbase/logs/hbase-regionserver-gc.log"
EOF

# Configure HBase for Ganglia
# overwrite hadoop-metrics.properties
cat > $HBASE_HOME/conf/hadoop-metrics.properties <<EOF
dfs.class=org.apache.hadoop.metrics.ganglia.GangliaContext
dfs.period=10
dfs.servers=$MASTER_HOST:8649
hbase.class=org.apache.hadoop.metrics.ganglia.GangliaContext
hbase.period=10
hbase.servers=$MASTER_HOST:8649
jvm.class=org.apache.hadoop.metrics.ganglia.GangliaContext
jvm.period=10
jvm.servers=$MASTER_HOST:8649
EOF

###############################################################################
# Start services
###############################################################################

# up open file descriptor limits
echo "root soft nofile 32768" >> /etc/security/limits.conf
echo "root hard nofile 32768" >> /etc/security/limits.conf

# up epoll limits
# ok if this fails, only valid for kernels 2.6.27+
sysctl -w fs.epoll.max_user_instances=32768 > /dev/null 2>&1

mkdir -p /mnt/hadoop/logs
mkdir -p /mnt/hbase/logs

[ ! -f /etc/hosts ] &&  echo "127.0.0.1 localhost" > /etc/hosts

# not set on boot
export USER="root"

if [ "$IS_MASTER" = "true" ]; then
  # MASTER
  # Prep Ganglia
  sed -i -e "s|\( *mcast_join *=.*\)|#\1|" \
         -e "s|\( *bind *=.*\)|#\1|" \
         -e "s|\( *mute *=.*\)|  mute = yes|" \
         -e "s|\( *location *=.*\)|  location = \"master-node\"|" \
         /etc/gmond.conf
  mkdir -p /mnt/ganglia/rrds
  chown -R ganglia:ganglia /mnt/ganglia/rrds
  rm -rf /var/lib/ganglia; cd /var/lib; ln -s /mnt/ganglia ganglia; cd
  service gmond start
  service gmetad start
  apachectl start

  # only format on first boot
  [ ! -e /mnt/hadoop/dfs ] && "$HADOOP_HOME"/bin/hadoop namenode -format

  "$HADOOP_HOME"/bin/hadoop-daemon.sh start namenode

  "$HADOOP_HOME"/bin/hadoop-daemon.sh start datanode

  "$HADOOP_HOME"/bin/hadoop-daemon.sh start jobtracker

  sleep 10

  "$HBASE_HOME"/bin/hbase-daemon.sh start master

else

  # SLAVE

  # Prep Ganglia
  sed -i -e "s|\( *mcast_join *=.*\)|#\1|" \
         -e "s|\( *bind *=.*\)|#\1|" \
         -e "s|\(udp_send_channel {\)|\1\n  host=$MASTER_HOST|" \
         /etc/gmond.conf
  service gmond start

  "$HADOOP_HOME"/bin/hadoop-daemon.sh start datanode

  "$HBASE_HOME"/bin/hbase-daemon.sh start regionserver

  "$HADOOP_HOME"/bin/hadoop-daemon.sh start tasktracker

fi

# Run this script on next boot
rm -f /var/ec2/ec2-run-user-data.*
