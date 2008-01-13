#!/usr/bin/ruby

$:.push('~/thrift/trunk/lib/rb/lib')
$:.push('./gen-rb')

require 'thrift/transport/tsocket'
require 'thrift/protocol/tbinaryprotocol'

require 'Hbase'

def printRow(row, values)
  print "row: #{row}, cols: "
  values.sort.each do |k,v|
    print "#{k} => #{v}; "
  end
  puts ""
end

def printEntry(entry)
  printRow(entry.row, entry.columns)
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
client.put(t, "foo", "entry:foo", invalid)

# try empty strings
client.put(t, "", "entry:", "");

# this row name is valid utf8
client.put(t, valid, "entry:foo", valid)

# non-utf8 is not allowed in row names
begin
  client.put(t, invalid, "entry:foo", invalid)
  raise "shouldn't get here!"
rescue Apache::Hadoop::Hbase::Thrift::IOError => e
  puts "expected error: #{e.message}"
end

# Run a scanner on the rows we just created
puts "Starting scanner..."
scanner = client.scannerOpen(t, "", ["entry:"])
begin
  while (true) 
    printEntry(client.scannerGet(scanner))
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

  client.put(t, row, "unused:", "DELETE_ME");
  printRow(row, client.getRow(t, row));
  client.deleteAllRow(t, row)

  client.put(t, row, "entry:num", "0")
  client.put(t, row, "entry:foo", "FOO")
  printRow(row, client.getRow(t, row));

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
  printRow(row, client.getRow(t, row));

  client.put(t, row, "entry:num", e.to_s)
  client.put(t, row, "entry:sqr", (e*e).to_s)
  printRow(row, client.getRow(t, row));
  
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
  printRow(row, client.getRow(t, row));

  versions = client.getVer(t, row, "entry:num", 10)
  print "row: #{row}, values: "
  versions.each do |v|
    print "#{v}; "
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
  columns << col
end

puts "Starting scanner..."
scanner = client.scannerOpenWithStop(t, "00020", "00040", columns)
begin
  while (true) 
    printEntry(client.scannerGet(scanner))
  end
rescue Apache::Hadoop::Hbase::Thrift::NotFound => nf
  client.scannerClose(scanner)
  puts "Scanner finished"
end
  
transport.close()
