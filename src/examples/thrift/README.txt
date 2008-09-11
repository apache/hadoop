Hbase Thrift Client Examples
============================

Included in this directory are sample clients of the HBase ThriftServer.  They
all perform the same actions but are implemented in C++, Java, Ruby, PHP, and
Python respectively.

To run/compile this clients, you will first need to install the thrift package
(from http://developers.facebook.com/thrift/) and then run thrift to generate
the language files:

thrift --gen cpp --gen java --gen rb --gen py -php \
    ../../../src/java/org/apache/hadoop/hbase/thrift/Hbase.thrift

See the individual DemoClient test files for more specific instructions on 
running each test.
