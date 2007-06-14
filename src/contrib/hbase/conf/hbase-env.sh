# Set HBase-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
# export JAVA_HOME=/usr/lib/j2sdk1.5-sun

# Extra Java CLASSPATH elements.  Optional.
# export HBASE_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
# export HBASE_HEAPSIZE=1000

# Extra Java runtime options.  Empty by default.
# export HBASE_OPTS=-server

# File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
# export HBASE_REGIONSERVERS=${HBASE_HOME}/conf/regionservers
