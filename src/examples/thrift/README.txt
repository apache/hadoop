Hbase Thrift Client Examples
============================

Included in this directory are sample clients of the HBase ThriftServer.  They
all perform the same actions but are implemented in C++, Java, Ruby, and PHP
respectively.

To run/compile this clients, you will first need to install the thrift package
(from http://developers.facebook.com/thrift/) and then run thrift to generate
the language files:

thrift -cpp -java -rb -php \
    ../../../src/java/org/apache/hadoop/hbase/thrift/Hbase.thrift

