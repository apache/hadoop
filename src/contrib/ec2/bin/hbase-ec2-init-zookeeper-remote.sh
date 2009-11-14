#!/usr/bin/env bash

# ZOOKEEPER_QUORUM set in the environment by the caller
HBASE_HOME=`ls -d /usr/local/hbase-*`

###############################################################################
# HBase configuration (Zookeeper)
###############################################################################

cat > $HBASE_HOME/conf/hbase-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>$ZOOKEEPER_QUORUM</value>
</property>
<property>
  <name>zookeeper.session.timeout</name>
  <value>60000</value>
</property>
<property>
  <name>hbase.zookeeper.property.dataDir</name>
  <value>/mnt/hbase/zk</value>
</property>
<property>
  <name>hbase.zookeeper.property.maxClientCnxns</name>
  <value>100</value>
</property>
</configuration>
EOF

###############################################################################
# Start services
###############################################################################

# up open file descriptor limits
echo "root soft nofile 32768" >> /etc/security/limits.conf
echo "root hard nofile 32768" >> /etc/security/limits.conf

# up epoll limits
# ok if this fails, only valid for kernels 2.6.27+
sysctl -w fs.epoll.max_user_instance=32768

mkdir -p /mnt/hbase/logs
mkdir -p /mnt/hbase/zk

[ ! -f /etc/hosts ] &&  echo "127.0.0.1 localhost" > /etc/hosts

"$HBASE_HOME"/bin/hbase-daemon.sh start zookeeper
