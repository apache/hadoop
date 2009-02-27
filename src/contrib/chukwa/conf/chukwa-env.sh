# Set Chukwa-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=/usr/lib/j2sdk1.5-sun

# The location of the Hadoop the collector should use. Default 
# assumes that this chukwa is living in hadoop's src/contrib directory
export HADOOP_HOME="/usr/lib/hadoop/current"
export HADOOP_CONF_DIR="${HADOOP_HOME}/conf/"

# The directory where pid files are stored. CHUKWA_HOME/var/run by default.
#export CHUKWA_PID_DIR=

# The location of chukwa logs, defaults to CHUKWA_HOME/logs
export CHUKWA_LOG_DIR=${CHUKWA_HOME}/logs

# The location of a hadoop jars. use this if you are running a collector
# without a running HDFS (i.e. which writes sequence files to local disk)
# if this is not set, the default is to check HADOOP_HOME for jars or
# classes, if those are not found, uses hadoop jars which come with chukwa
export HADOOP_JAR=`ls ${HADOOP_HOME}/hadoop-*-core.jar`

# The location of chukwa data repository
export chuwaRecordsRepository="/chukwa/repos/"

# The location of torque pbsnodes command
export nodeActivityCmde="/usr/lib/torque/current/bin/pbsnodes "

# The server which contain pbsnodes, qstat and tracejob.
export TORQUE_SERVER=localhost

# The location contain torque binaries.
export TORQUE_HOME=/usr/lib/torque

# The domain of the cluster
#export DOMAIN=

# Instance name for chukwa deployment
export CHUKWA_IDENT_STRING=demo

# JAVA LIBRARY PATH for native compression codec
export JAVA_PLATFORM=Linux-i386-32
export JAVA_LIBRARY_PATH=${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}

