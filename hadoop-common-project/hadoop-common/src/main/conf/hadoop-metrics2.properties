# syntax: [prefix].[source|sink].[instance].[options]
# See javadoc of package-info.java for org.apache.hadoop.metrics2 for details

*.sink.file.class=org.apache.hadoop.metrics2.sink.FileSink
# default sampling period, in seconds
*.period=10

# The namenode-metrics.out will contain metrics from all context
#namenode.sink.file.filename=namenode-metrics.out
# Specifying a special sampling period for namenode:
#namenode.sink.*.period=8

#datanode.sink.file.filename=datanode-metrics.out

#resourcemanager.sink.file.filename=resourcemanager-metrics.out

#nodemanager.sink.file.filename=nodemanager-metrics.out

#mrappmaster.sink.file.filename=mrappmaster-metrics.out

#jobhistoryserver.sink.file.filename=jobhistoryserver-metrics.out

# the following example split metrics of different
# context to different sinks (in this case files)
#nodemanager.sink.file_jvm.class=org.apache.hadoop.metrics2.sink.FileSink
#nodemanager.sink.file_jvm.context=jvm
#nodemanager.sink.file_jvm.filename=nodemanager-jvm-metrics.out
#nodemanager.sink.file_mapred.class=org.apache.hadoop.metrics2.sink.FileSink
#nodemanager.sink.file_mapred.context=mapred
#nodemanager.sink.file_mapred.filename=nodemanager-mapred-metrics.out

#
# Below are for sending metrics to Ganglia
#
# for Ganglia 3.0 support
# *.sink.ganglia.class=org.apache.hadoop.metrics2.sink.ganglia.GangliaSink30
#
# for Ganglia 3.1 support
# *.sink.ganglia.class=org.apache.hadoop.metrics2.sink.ganglia.GangliaSink31

# *.sink.ganglia.period=10

# default for supportsparse is false
# *.sink.ganglia.supportsparse=true

#*.sink.ganglia.slope=jvm.metrics.gcCount=zero,jvm.metrics.memHeapUsedM=both
#*.sink.ganglia.dmax=jvm.metrics.threadsBlocked=70,jvm.metrics.memHeapUsedM=40

# Tag values to use for the ganglia prefix. If not defined no tags are used.
# If '*' all tags are used. If specifiying multiple tags separate them with 
# commas. Note that the last segment of the property name is the context name.
#
#*.sink.ganglia.tagsForPrefix.jvm=ProcesName
#*.sink.ganglia.tagsForPrefix.dfs=
#*.sink.ganglia.tagsForPrefix.rpc=
#*.sink.ganglia.tagsForPrefix.mapred=

#namenode.sink.ganglia.servers=yourgangliahost_1:8649,yourgangliahost_2:8649

#datanode.sink.ganglia.servers=yourgangliahost_1:8649,yourgangliahost_2:8649

#resourcemanager.sink.ganglia.servers=yourgangliahost_1:8649,yourgangliahost_2:8649

#nodemanager.sink.ganglia.servers=yourgangliahost_1:8649,yourgangliahost_2:8649

#mrappmaster.sink.ganglia.servers=yourgangliahost_1:8649,yourgangliahost_2:8649

#jobhistoryserver.sink.ganglia.servers=yourgangliahost_1:8649,yourgangliahost_2:8649
