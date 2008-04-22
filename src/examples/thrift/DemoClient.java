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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.SortedMap;

import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.NotFound;
import org.apache.hadoop.hbase.thrift.generated.ScanEntry;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;

public class DemoClient {
  
  protected int port = 9090;
  CharsetDecoder decoder = null;

  public static void main(String[] args) 
    throws IOError, TException, NotFound, UnsupportedEncodingException, IllegalArgument, AlreadyExists 
  {
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
    AbstractMap<byte[], ColumnDescriptor> columnMap = client.getColumnDescriptors(t);
    for (ColumnDescriptor col2 : columnMap.values()) {
      System.out.println("  column: " + utf8(col2.name) + ", maxVer: " + Integer.toString(col2.maxVersions));
    }
    
    //
    // Test UTF-8 handling
    //
    byte[] invalid = { (byte) 'f', (byte) 'o', (byte) 'o', (byte) '-', (byte) 0xfc, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1 };
    byte[] valid = { (byte) 'f', (byte) 'o', (byte) 'o', (byte) '-', (byte) 0xE7, (byte) 0x94, (byte) 0x9F, (byte) 0xE3, (byte) 0x83, (byte) 0x93, (byte) 0xE3, (byte) 0x83, (byte) 0xBC, (byte) 0xE3, (byte) 0x83, (byte) 0xAB};

    // non-utf8 is fine for data
    client.put(t, bytes("foo"), bytes("entry:foo"), invalid);

    // try empty strings
    client.put(t, bytes(""), bytes("entry:"), bytes(""));
    
    // this row name is valid utf8
    client.put(t, valid, bytes("entry:foo"), valid);
    
    // non-utf8 is not allowed in row names
    try {
      client.put(t, invalid, bytes("entry:foo"), invalid);
      System.out.println("FATAL: shouldn't get here");
      System.exit(-1);
    } catch (IOError e) {
      System.out.println("expected error: " + e.message);
    }
    
    // Run a scanner on the rows we just created
    ArrayList<byte[]> columnNames = new ArrayList<byte[]>();
    columnNames.add(bytes("entry:"));
    
    System.out.println("Starting scanner...");
    int scanner = client
        .scannerOpen(t, bytes(""), columnNames);
    try {
      while (true) {
        ScanEntry value = client.scannerGet(scanner);
        printEntry(value);
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
      
      client.put(t, row, bytes("unused:"), bytes("DELETE_ME"));
      printRow(row, client.getRow(t, row));
      client.deleteAllRow(t, row);

      client.put(t, row, bytes("entry:num"), bytes("0"));
      client.put(t, row, bytes("entry:foo"), bytes("FOO"));
      printRow(row, client.getRow(t, row));

      Mutation m = null;      
      ArrayList<Mutation> mutations = new ArrayList<Mutation>();
      m = new Mutation();
      m.column = bytes("entry:foo");
      m.isDelete = true;
      mutations.add(m);
      m = new Mutation();
      m.column = bytes("entry:num");
      m.value = bytes("-1");
      mutations.add(m);
      client.mutateRow(t, row, mutations);
      printRow(row, client.getRow(t, row));
      
      client.put(t, row, bytes("entry:num"), bytes(Integer.toString(i)));
      client.put(t, row, bytes("entry:sqr"), bytes(Integer.toString(i * i)));
      printRow(row, client.getRow(t, row));

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
      printRow(row, client.getRow(t, row));

      ArrayList<byte[]> versions = client.getVer(t, row, bytes("entry:num"), 10);
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
      columnNames.add(col2.name);
    }
    
    System.out.println("Starting scanner...");
    scanner = client.scannerOpenWithStop(t, bytes("00020"), bytes("00040"),
        columnNames);
    try {
      while (true) {
        ScanEntry value = client.scannerGet(scanner);
        printEntry(value);
      }
    } catch (NotFound nf) {
      client.scannerClose(scanner);
      System.out.println("Scanner finished");
    }
    
    transport.close();
  }
  
  private final void printVersions(byte[] row, ArrayList<byte[]> values) {
    StringBuilder rowStr = new StringBuilder();
    for (byte[] value : values) {
      rowStr.append(utf8(value));
      rowStr.append("; ");
    }
    System.out.println("row: " + utf8(row) + ", values: " + rowStr);
  }
  
  private final void printEntry(ScanEntry entry) {
    printRow(entry.row, entry.columns);
  }
  
  private final void printRow(byte[] row, AbstractMap<byte[], byte[]> values) {
    // copy values into a TreeMap to get them in sorted order
    
    TreeMap<String,byte[]> sorted = new TreeMap<String,byte[]>();
    for (AbstractMap.Entry<byte[], byte[]> entry : values.entrySet()) {
      sorted.put(utf8(entry.getKey()), entry.getValue());
    }
    
    StringBuilder rowStr = new StringBuilder();
    for (SortedMap.Entry<String, byte[]> entry : sorted.entrySet()) {
      rowStr.append(entry.getKey());
      rowStr.append(" => ");
      rowStr.append(utf8(entry.getValue()));
      rowStr.append("; ");
    }
    System.out.println("row: " + utf8(row) + ", cols: " + rowStr);
  }
}
