@echo off
@rem Set Hadoop-specific environment variables here.

@rem The only required environment variable is JAVA_HOME.  All others are
@rem optional.  When running a distributed configuration it is best to
@rem set JAVA_HOME in this file, so that it is correctly defined on
@rem remote nodes.

@rem The java implementation to use.  Required.

@rem Extra Java CLASSPATH elements.  Optional.
@rem set HADOOP_CLASSPATH=

@rem The maximum amount of heap to use, in MB. Default is 1000.
@rem set HADOOP_HEAPSIZE=2000

@rem Extra Java runtime options.  Empty by default.
@rem set HADOOP_OPTS=-server

@rem Command specific options appended to HADOOP_OPTS when specified
set HADOOP_NAMENODE_OPTS=-Dcom.sun.management.jmxremote %HADOOP_NAMENODE_OPTS%
set HADOOP_SECONDARYNAMENODE_OPTS=-Dcom.sun.management.jmxremote %HADOOP_SECONDARYNAMENODE_OPTS%
set HADOOP_DATANODE_OPTS=-Dcom.sun.management.jmxremote %HADOOP_DATANODE_OPTS%
set HADOOP_BALANCER_OPTS=-Dcom.sun.management.jmxremote %HADOOP_BALANCER_OPTS%
set HADOOP_JOBTRACKER_OPTS=-Dcom.sun.management.jmxremote %HADOOP_JOBTRACKER_OPTS%
set HADOOP_TASKTRACKER_OPTS=-Dcom.sun.management.jmxremote %HADOOP_TASKTRACKER_OPTS%

@rem The following applies to multiple commands (fs, dfs, fsck, distcp etc)
@rem set HADOOP_CLIENT_OPTS

@rem File naming remote slave hosts.  HADOOP_HOME\conf\slaves by default.
@rem set HADOOP_SLAVES=%HADOOP_HOME%\conf\slaves

@rem host:path where hadoop code should be rsync'd from.  Unset by default.
@rem set HADOOP_MASTER=master:c:\apps\src\hadoop

@rem A string representing this instance of hadoop. %USERNAME% by default.
@rem set HADOOP_IDENT_STRING=%USERNAME%
