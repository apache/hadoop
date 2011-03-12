#!/usr/bin/ruby

# Copyright 2008 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Instructions: 
# 1. Run Thrift to generate the ruby module HBase
#    thrift --gen rb ../../../src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift
# 2. Modify the import string below to point to {$THRIFT_HOME}/lib/rb/lib.
# 3. Execute {ruby DemoClient.rb}.

# You will need to modify this import string:
$:.push('~/Thrift/thrift-20080411p1/lib/rb/lib')
$:.push('./gen-rb')

require 'thrift/transport/tsocket'
require 'thrift/protocol/tbinaryprotocol'

require 'Hbase'

def printRow(rowresult)
  print "row: #{rowresult.row}, cols: "
  rowresult.columns.sort.each do |k,v|
    print "#{k} => #{v.value}; "
  end
  puts ""
end

transport = TBufferedTransport.new(TSocket.new("localhost", 9090))
protocol = TBinaryProtocol.new(transport)
client = Apache::Hadoop::Hbase::Thrift::Hbase::Client.new(protocol)

transport.open()

t = "demo_table"

#
# Scan all tables, look for the demo table and delete it.
#
puts "scanning tables..."
client.getTableNames().sort.each do |name|
  puts "  found: #{name}"
  if (name == t)
    if (client.isTableEnabled(name))
      puts "    disabling table: #{name}"
      client.disableTable(name)
    end
    puts "    deleting table: #{name}" 
    client.deleteTable(name)
  end
end

#
# Create the demo table with two column families, entry: and unused:
#
columns = []
col = Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new
col.name = "entry:"
col.maxVersions = 10
columns << col;
col = Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new
col.name = "unused:"
columns << col;

puts "creating table: #{t}"
begin
  client.createTable(t, columns)
rescue Apache::Hadoop::Hbase::Thrift::AlreadyExists => ae
  puts "WARN: #{ae.message}"
end

puts "column families in #{t}: "
client.getColumnDescriptors(t).sort.each do |key, col|
  puts "  column: #{col.name}, maxVer: #{col.maxVersions}"
end

#
# Test UTF-8 handling
#
invalid = "foo-\xfc\xa1\xa1\xa1\xa1\xa1"
valid = "foo-\xE7\x94\x9F\xE3\x83\x93\xE3\x83\xBC\xE3\x83\xAB";

# non-utf8 is fine for data
mutations = []
m = Apache::Hadoop::Hbase::Thrift::Mutation.new
m.column = "entry:foo"
m.value = invalid
mutations << m
client.mutateRow(t, "foo", mutations)

# try empty strings
mutations = []
m = Apache::Hadoop::Hbase::Thrift::Mutation.new
m.column = "entry:"
m.value = ""
mutations << m
client.mutateRow(t, "", mutations)

# this row name is valid utf8
mutations = []
m = Apache::Hadoop::Hbase::Thrift::Mutation.new
m.column = "entry:foo"
m.value = valid
mutations << m
client.mutateRow(t, valid, mutations)

# non-utf8 is not allowed in row names
begin
  mutations = []
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:foo"
  m.value = invalid
  mutations << m
  client.mutateRow(t, invalid, mutations)
  raise "shouldn't get here!"
rescue Apache::Hadoop::Hbase::Thrift::IOError => e
  puts "expected error: #{e.message}"
end

# Run a scanner on the rows we just created
puts "Starting scanner..."
scanner = client.scannerOpen(t, "", ["entry:"])
begin
  while (true) 
    printRow(client.scannerGet(scanner))
  end
rescue Apache::Hadoop::Hbase::Thrift::NotFound => nf
  client.scannerClose(scanner)
  puts "Scanner finished"
end

#
# Run some operations on a bunch of rows.
#
(0..100).to_a.reverse.each do |e|
  # format row keys as "00000" to "00100"
  row = format("%0.5d", e)

  mutations = []
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "unused:"
  m.value = "DELETE_ME"
  mutations << m
  client.mutateRow(t, row, mutations)
  printRow(client.getRow(t, row))
  client.deleteAllRow(t, row)

  mutations = []
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:num"
  m.value = "0"
  mutations << m
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:foo"
  m.value = "FOO"
  mutations << m
  client.mutateRow(t, row, mutations)
  printRow(client.getRow(t, row))

  mutations = []
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:foo"
  m.isDelete = 1
  mutations << m
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:num"
  m.value = "-1"
  mutations << m
  client.mutateRow(t, row, mutations)
  printRow(client.getRow(t, row));

  mutations = []
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:num"
  m.value = e.to_s
  mutations << m
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:sqr"
  m.value = (e*e).to_s
  mutations << m
  client.mutateRow(t, row, mutations)
  printRow(client.getRow(t, row))
  
  mutations = []
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:num"
  m.value = "-999"
  mutations << m
  m = Apache::Hadoop::Hbase::Thrift::Mutation.new
  m.column = "entry:sqr"
  m.isDelete = 1
  mutations << m
  client.mutateRowTs(t, row, mutations, 1) # shouldn't override latest
  printRow(client.getRow(t, row));

  versions = client.getVer(t, row, "entry:num", 10)
  print "row: #{row}, values: "
  versions.each do |v|
    print "#{v.value}; "
  end
  puts ""    
  
  begin
    client.get(t, row, "entry:foo")
    raise "shouldn't get here!"
  rescue Apache::Hadoop::Hbase::Thrift::NotFound => nf
    # blank
  end

  puts ""
end 

columns = []
client.getColumnDescriptors(t).each do |col, desc|
  puts "column with name: #{desc.name}"
  columns << desc.name + ":"
end

puts "Starting scanner..."
scanner = client.scannerOpenWithStop(t, "00020", "00040", columns)
begin
  while (true) 
    printRow(client.scannerGet(scanner))
  end
rescue Apache::Hadoop::Hbase::Thrift::NotFound => nf
  client.scannerClose(scanner)
  puts "Scanner finished"
end
  
transport.close()
