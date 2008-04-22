#!/usr/bin/python
'''Copyright 2008 The Apache Software Foundation
 
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
''' 
# Instructions:
# 1. Run Thrift to generate python module HBase
#    thrift -py ../../../src/java/org/apache/hadoop/hbase/thrift/Hbase.thrift 
# 2. Rename gen-py folder to gen_py or just copy gen-py/HBase module into your project tree and change import string
# Contributed by: Ivan Begtin (ibegtin@gmail.com, ibegtin@enotpoiskun.ru)


from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from gen_py.Hbase import *

def printRow(row, values):
    print "row: %s, cols: " %(row)
    for key in values.keys():
	print '\t%s => %s' %(key, values[key])

def printEntry(entry):
    printRow(entry.row, entry.columns)


# Make socket
transport = TSocket.TSocket('localhost', 9090)

# Buffering is critical. Raw sockets are very slow
transport = TTransport.TBufferedTransport(transport)

# Wrap in a protocol
protocol = TBinaryProtocol.TBinaryProtocol(transport)

# Create a client to use the protocol encoder
client = Hbase.Client(protocol)

# Connect!
transport.open()

t = "demo_table"

#
# Scan all tables, look for the demo table and delete it.
#
print "scanning tables..."
for table in client.getTableNames():
    print "  found: %s" %(table)
    if table == t:
	print "    deleting table: %s"  %(t)	
	client.deleteTable(table)

columns = []
col = Hbase.ColumnDescriptor()
col.name = 'entry:'
col.maxVersions = 10
columns.append(col)
col = Hbase.ColumnDescriptor()
col.name = 'unused:'
columns.append(col)

client.createTable(t, columns)

cols = client.getColumnDescriptors(t)
for col_name in cols.keys():
    col = cols[col_name]
    print "  column: %s, maxVer: %d" % (col.name, col.maxVersions)
#
# Test UTF-8 handling
#
invalid = "foo-\xfc\xa1\xa1\xa1\xa1\xa1"
valid = "foo-\xE7\x94\x9F\xE3\x83\x93\xE3\x83\xBC\xE3\x83\xAB";

# non-utf8 is fine for data
client.put(t, "foo", "entry:foo", invalid)

# try empty strings
client.put(t, "", "entry:", "");

# this row name is valid utf8
client.put(t, valid, "entry:foo", valid)

# non-utf8 is not allowed in row names
try:
    client.put(t, invalid, "entry:foo", invalid)
except ttypes.IOError, e:
    print 'expected exception: %s' %(e.message)

# Run a scanner on the rows we just created
print "Starting scanner..."
scanner = client.scannerOpen(t, "", ["entry:"])
try:
    while 1:
	printEntry(client.scannerGet(scanner))
except ttypes.NotFound, e:
    print "Scanner finished"
  

#
# Run some operations on a bunch of rows.
#
for e in range(100, 0, -1):
  # format row keys as "00000" to "00100"
    row = "%0.5d" % (e)

    client.put(t, row, "unused:", "DELETE_ME");
    printRow(row, client.getRow(t, row));
    client.deleteAllRow(t, row)

    client.put(t, row, "entry:num", "0")
    client.put(t, row, "entry:foo", "FOO")
    printRow(row, client.getRow(t, row));

    mutations = []
    m = Hbase.Mutation()
    m.column = "entry:foo"
    m.isDelete = 1
    mutations.append(m)
    m = Hbase.Mutation()
    m.column = "entry:num"
    m.value = "-1"
    mutations.append(m)
    client.mutateRow(t, row, mutations)
    printRow(row, client.getRow(t, row));

    client.put(t, row, "entry:num", str(e))
    client.put(t, row, "entry:sqr", str((e*e)))
    printRow(row, client.getRow(t, row));
  
    mutations = []
    m = Hbase.Mutation()
    m.column = "entry:num"
    m.value = "-999"
    mutations.append(m)
    m = Hbase.Mutation()
    m.column = "entry:sqr"
    m.isDelete = 1
    mutations.append(m)
    client.mutateRowTs(t, row, mutations, 1) # shouldn't override latest
    printRow(row, client.getRow(t, row))

    versions = client.getVer(t, row, "entry:num", 10)
    print "row: %s, values: " %(row)
    for v in versions:
	print "\t%s;" %(v)

    print ""
  
    try:
	client.get(t, row, "entry:foo")
	raise "shouldn't get here!"
    except ttypes.NotFound, e:
	pass

    print ""


columns = client.getColumnDescriptors(t)

print "Starting scanner..."
scanner = client.scannerOpenWithStop(t, "00020", "00040", columns)
try:
  while 1:
    printEntry(client.scannerGet(scanner))
except ttypes.NotFound, e:
    client.scannerClose(scanner)
    print "Scanner finished"
  
transport.close()
