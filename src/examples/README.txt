Example code.

* src/examples/thrift
    Examples for interacting with HBase via Thrift from C++, PHP, Python and Ruby.
* org.apache.hadoop.hbase.mapreduce.SampleUploader
    Demonstrates uploading data from text files (presumably stored in HDFS) to HBase.
* org.apache.hadoop.hbase.mapreduce.IndexBuilder
    Demonstrates map/reduce with a table as the source and other tables as the sink.

As of 0.20 there is no ant target for building the examples. You can easily build
the Java examples by copying them to the right location in the main source hierarchy.