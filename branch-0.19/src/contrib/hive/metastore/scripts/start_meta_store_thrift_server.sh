#!/bin/sh


CLASSPATH=

# the dist lib libraries
for f in /usr/local/fbprojects/hive.metastore/lib/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# the hadoop libraries
for f in /mnt/hive/stable/cluster/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# the apache libraries
for f in /mnt/hive/stable/cluster/lib/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# for now, the fb_hive libraries
for f in /mnt/hive/stable/lib/hive/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

/usr/local/bin/java -Dcom.sun.management.jmxremote -Djava.library.path=/mnt/hive/production/cluster/lib/native/Linux-amd64-64/ -cp $CLASSPATH com.facebook.metastore.MetaStoreServer
