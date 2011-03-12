/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.thrift;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.SortedMap;

import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.NotFound;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;

/*
 * Instructions:
 * 1. Run Thrift to generate the java module HBase
 *    thrift --gen java ../../../src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift
 * 2. Acquire a jar of compiled Thrift java classes.  As of this writing, HBase ships 
 *    with this jar (libthrift-[VERSION].jar).  If this jar is not present, or it is 
 *    out-of-date with your current version of thrift, you can compile the jar 
 *    yourself by executing {ant} in {$THRIFT_HOME}/lib/java.
 * 3. Compile and execute this file with both the libthrift jar and the gen-java/ 
 *    directory in the classpath.  This can be done on the command-line with the 
 *    following lines: (from the directory containing this file and gen-java/)
 *    
 *    javac -cp /path/to/libthrift/jar.jar:gen-java/ DemoClient.java
 *    mv DemoClient.class gen-java/org/apache/hadoop/hbase/thrift/
 *    java -cp /path/to/libthrift/jar.jar:gen-java/ org.apache.hadoop.hbase.thrift.DemoClient
 * 
 */
public class DemoClient {
  
  protected int port = 9090;
  CharsetDecoder decoder = null;

  public static void main(String[] args) 
  throws IOError, TException, NotFound, UnsupportedEncodingException, IllegalArgument, AlreadyExists {
    DemoClient client = new DemoClient();
    client.run();
  }

  DemoClient() {
    decoder = Charset.forName("UTF-8").newDecoder();
  }
  
  // Helper to translate byte[]'s to UTF8 strings
  private String utf8(byte[] buf) {
    try {
      return decoder.decode(ByteBuffer.wrap(buf)).toString();
    } catch (CharacterCodingException e) {
      return "[INVALID UTF-8]";
    }
  }
  
  // Helper to translate strings to UTF8 bytes
  private byte[] bytes(String s) {
    try {
      return s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  private void run() throws IOError, TException, NotFound, IllegalArgument,
      AlreadyExists {
    
    TTransport transport = new TSocket("localhost", port);
    TProtocol protocol = new TBinaryProtocol(transport, true, true);
    Hbase.Client client = new Hbase.Client(protocol);

    transport.open();

    byte[] t = bytes("demo_table");
    
    //
    // Scan all tables, look for the demo table and delete it.
    //
    System.out.println("scanning tables...");
    for (byte[] name : client.getTableNames()) {
      System.out.println("  found: " + utf8(name));
      if (utf8(name).equals(utf8(t))) {
        if (client.isTableEnabled(name)) {
          System.out.println("    disabling table: " + utf8(name));
          client.disableTable(name);
        }
        System.out.println("    deleting table: " + utf8(name)); 
        client.deleteTable(name);
      }
    }
    
    //
    // Create the demo table with two column families, entry: and unused:
    //
    ArrayList<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
    ColumnDescriptor col = null;
    col = new ColumnDescriptor();
    col.name = bytes("entry:");
    col.maxVersions = 10;
    columns.add(col);
    col = new ColumnDescriptor();
    col.name = bytes("unused:");
    columns.add(col);

    System.out.println("creating table: " + utf8(t));
    try {
      client.createTable(t, columns);
    } catch (AlreadyExists ae) {
      System.out.println("WARN: " + ae.message);
    }
    
    System.out.println("column families in " + utf8(t) + ": ");
    Map<byte[], ColumnDescriptor> columnMap = client.getColumnDescriptors(t);
    for (ColumnDescriptor col2 : columnMap.values()) {
      System.out.println("  column: " + utf8(col2.name) + ", maxVer: " + Integer.toString(col2.maxVersions));
    }
    
    //
    // Test UTF-8 handling
    //
    byte[] invalid = { (byte) 'f', (byte) 'o', (byte) 'o', (byte) '-', (byte) 0xfc, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1 };
    byte[] valid = { (byte) 'f', (byte) 'o', (byte) 'o', (byte) '-', (byte) 0xE7, (byte) 0x94, (byte) 0x9F, (byte) 0xE3, (byte) 0x83, (byte) 0x93, (byte) 0xE3, (byte) 0x83, (byte) 0xBC, (byte) 0xE3, (byte) 0x83, (byte) 0xAB};

    ArrayList<Mutation> mutations;
    // non-utf8 is fine for data
    mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, bytes("entry:foo"), invalid));
    client.mutateRow(t, bytes("foo"), mutations);

    // try empty strings
    mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, bytes("entry:"), bytes("")));
    client.mutateRow(t, bytes(""), mutations);

    // this row name is valid utf8
    mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, bytes("entry:foo"), valid));
    client.mutateRow(t, valid, mutations);
    
    // non-utf8 is not allowed in row names
    try {
      mutations = new ArrayList<Mutation>();
      mutations.add(new Mutation(false, bytes("entry:foo"), invalid));
      client.mutateRow(t, invalid, mutations);
      System.out.println("FATAL: shouldn't get here");
      System.exit(-1);
    } catch (IOError e) {
      System.out.println("expected error: " + e.message);
    }
    
    // Run a scanner on the rows we just created
    ArrayList<byte[]> columnNames = new ArrayList<byte[]>();
    columnNames.add(bytes("entry:"));
    
    System.out.println("Starting scanner...");
    int scanner = client.scannerOpen(t, bytes(""), columnNames);
    try {
      while (true) {
        TRowResult entry = client.scannerGet(scanner);
        printRow(entry);
      }
    } catch (NotFound nf) {
      client.scannerClose(scanner);
      System.out.println("Scanner finished");
    }
    
    //
    // Run some operations on a bunch of rows
    //
    for (int i = 100; i >= 0; --i) {
      // format row keys as "00000" to "00100"
      NumberFormat nf = NumberFormat.getInstance();
      nf.setMinimumIntegerDigits(5);
      nf.setGroupingUsed(false);
      byte[] row = bytes(nf.format(i));
      
      mutations = new ArrayList<Mutation>();
      mutations.add(new Mutation(false, bytes("unused:"), bytes("DELETE_ME")));
      client.mutateRow(t, row, mutations);
      printRow(client.getRow(t, row));
      client.deleteAllRow(t, row);

      mutations = new ArrayList<Mutation>();
      mutations.add(new Mutation(false, bytes("entry:num"), bytes("0")));
      mutations.add(new Mutation(false, bytes("entry:foo"), bytes("FOO")));
      client.mutateRow(t, row, mutations);
      printRow(client.getRow(t, row));

      Mutation m = null;
      mutations = new ArrayList<Mutation>();
      m = new Mutation();
      m.column = bytes("entry:foo");
      m.isDelete = true;
      mutations.add(m);
      m = new Mutation();
      m.column = bytes("entry:num");
      m.value = bytes("-1");
      mutations.add(m);
      client.mutateRow(t, row, mutations);
      printRow(client.getRow(t, row));
      
      mutations = new ArrayList<Mutation>();
      mutations.add(new Mutation(false, bytes("entry:num"), bytes(Integer.toString(i))));
      mutations.add(new Mutation(false, bytes("entry:sqr"), bytes(Integer.toString(i * i))));
      client.mutateRow(t, row, mutations);
      printRow(client.getRow(t, row));

      // sleep to force later timestamp 
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        // no-op
      }
      
      mutations.clear();
      m = new Mutation();
      m.column = bytes("entry:num");
      m.value = bytes("-999");
      mutations.add(m);
      m = new Mutation();
      m.column = bytes("entry:sqr");
      m.isDelete = true;
      client.mutateRowTs(t, row, mutations, 1); // shouldn't override latest
      printRow(client.getRow(t, row));

      List<TCell> versions = client.getVer(t, row, bytes("entry:num"), 10);
      printVersions(row, versions);
      if (versions.size() != 4) {
        System.out.println("FATAL: wrong # of versions");
        System.exit(-1);
      }
      
      try {
        client.get(t, row, bytes("entry:foo"));
        System.out.println("FATAL: shouldn't get here");
        System.exit(-1);
      } catch (NotFound nf2) {
        // blank
      }

      System.out.println("");
    }
    
    // scan all rows/columnNames
    
    columnNames.clear();
    for (ColumnDescriptor col2 : client.getColumnDescriptors(t).values()) {
      System.out.println("column with name: " + new String(col2.name));
      System.out.println(col2.toString());
      columnNames.add((utf8(col2.name) + ":").getBytes());
    }
    
    System.out.println("Starting scanner...");
    scanner = client.scannerOpenWithStop(t, bytes("00020"), bytes("00040"),
        columnNames);
    try {
      while (true) {
        TRowResult entry = client.scannerGet(scanner);
        printRow(entry);
      }
    } catch (NotFound nf) {
      client.scannerClose(scanner);
      System.out.println("Scanner finished");
    }
    
    transport.close();
  }
  
  private final void printVersions(byte[] row, List<TCell> versions) {
    StringBuilder rowStr = new StringBuilder();
    for (TCell cell : versions) {
      rowStr.append(utf8(cell.value));
      rowStr.append("; ");
    }
    System.out.println("row: " + utf8(row) + ", values: " + rowStr);
  }
  
  private final void printRow(TRowResult rowResult) {
    // copy values into a TreeMap to get them in sorted order
    
    TreeMap<String,TCell> sorted = new TreeMap<String,TCell>();
    for (Map.Entry<byte[], TCell> column : rowResult.columns.entrySet()) {
      sorted.put(utf8(column.getKey()), column.getValue());
    }
    
    StringBuilder rowStr = new StringBuilder();
    for (SortedMap.Entry<String, TCell> entry : sorted.entrySet()) {
      rowStr.append(entry.getKey());
      rowStr.append(" => ");
      rowStr.append(utf8(entry.getValue().value));
      rowStr.append("; ");
    }
    System.out.println("row: " + utf8(rowResult.row) + ", cols: " + rowStr);
  }
}
