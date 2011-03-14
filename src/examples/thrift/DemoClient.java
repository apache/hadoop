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
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

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

    static protected int port;
    static protected String host;
    CharsetDecoder decoder = null;

    public static void main(String[] args)
            throws IOError, TException, UnsupportedEncodingException, IllegalArgument, AlreadyExists {

        if (args.length != 2) {
            
            System.out.println("Invalid arguments!");
            System.out.println("Usage: DemoClient host port");

            System.exit(-1);
        }

        port = Integer.parseInt(args[1]);
        host = args[0];


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

    private void run() throws IOError, TException, IllegalArgument,
            AlreadyExists {

        TTransport transport = new TSocket(host, port);
        TProtocol protocol = new TBinaryProtocol(transport, true, true);
        Hbase.Client client = new Hbase.Client(protocol);

        transport.open();

        byte[] t = bytes("demo_table");

        //
        // Scan all tables, look for the demo table and delete it.
        //
        System.out.println("scanning tables...");
        for (ByteBuffer name : client.getTableNames()) {
            System.out.println("  found: " + utf8(name.array()));
            if (utf8(name.array()).equals(utf8(t))) {
                if (client.isTableEnabled(name)) {
                    System.out.println("    disabling table: " + utf8(name.array()));
                    client.disableTable(name);
                }
                System.out.println("    deleting table: " + utf8(name.array()));
                client.deleteTable(name);
            }
        }

        //
        // Create the demo table with two column families, entry: and unused:
        //
        ArrayList<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
        ColumnDescriptor col = null;
        col = new ColumnDescriptor();
        col.name = ByteBuffer.wrap(bytes("entry:"));
        col.maxVersions = 10;
        columns.add(col);
        col = new ColumnDescriptor();
        col.name = ByteBuffer.wrap(bytes("unused:"));
        columns.add(col);

        System.out.println("creating table: " + utf8(t));
        try {
            client.createTable(ByteBuffer.wrap(t), columns);
        } catch (AlreadyExists ae) {
            System.out.println("WARN: " + ae.message);
        }

        System.out.println("column families in " + utf8(t) + ": ");
        Map<ByteBuffer, ColumnDescriptor> columnMap = client.getColumnDescriptors(ByteBuffer.wrap(t));
        for (ColumnDescriptor col2 : columnMap.values()) {
            System.out.println("  column: " + utf8(col2.name.array()) + ", maxVer: " + Integer.toString(col2.maxVersions));
        }

        //
        // Test UTF-8 handling
        //
        byte[] invalid = {(byte) 'f', (byte) 'o', (byte) 'o', (byte) '-', (byte) 0xfc, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1};
        byte[] valid = {(byte) 'f', (byte) 'o', (byte) 'o', (byte) '-', (byte) 0xE7, (byte) 0x94, (byte) 0x9F, (byte) 0xE3, (byte) 0x83, (byte) 0x93, (byte) 0xE3, (byte) 0x83, (byte) 0xBC, (byte) 0xE3, (byte) 0x83, (byte) 0xAB};

        ArrayList<Mutation> mutations;
        // non-utf8 is fine for data
        mutations = new ArrayList<Mutation>();
        mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")), ByteBuffer.wrap(invalid)));
        client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(bytes("foo")), mutations);

        // try empty strings
        mutations = new ArrayList<Mutation>();
        mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:")), ByteBuffer.wrap(bytes(""))));
        client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(bytes("")), mutations);

        // this row name is valid utf8
        mutations = new ArrayList<Mutation>();
        mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")), ByteBuffer.wrap(valid)));
        client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(valid), mutations);

        // non-utf8 is now allowed in row names because HBase stores values as binary
        ByteBuffer bf = ByteBuffer.wrap(invalid);

        mutations = new ArrayList<Mutation>();
        mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")), ByteBuffer.wrap(invalid)));
        client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(invalid), mutations);


        // Run a scanner on the rows we just created
        ArrayList<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        columnNames.add(ByteBuffer.wrap(bytes("entry:")));

        System.out.println("Starting scanner...");
        int scanner = client.scannerOpen(ByteBuffer.wrap(t), ByteBuffer.wrap(bytes("")), columnNames);
        
        while (true) {
            List<TRowResult> entry = client.scannerGet(scanner);
            if (entry.isEmpty()) {
                break;
            }
            printRow(entry);
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
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("unused:")), ByteBuffer.wrap(bytes("DELETE_ME"))));
            client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations);
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row)));
            client.deleteAllRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row));

            mutations = new ArrayList<Mutation>();
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:num")), ByteBuffer.wrap(bytes("0"))));
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")), ByteBuffer.wrap(bytes("FOO"))));
            client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations);
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row)));

            Mutation m = null;
            mutations = new ArrayList<Mutation>();
            m = new Mutation();
            m.column = ByteBuffer.wrap(bytes("entry:foo"));
            m.isDelete = true;
            mutations.add(m);
            m = new Mutation();
            m.column = ByteBuffer.wrap(bytes("entry:num"));
            m.value = ByteBuffer.wrap(bytes("-1"));
            mutations.add(m);
            client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations);
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row)));

            mutations = new ArrayList<Mutation>();
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:num")), ByteBuffer.wrap(bytes(Integer.toString(i)))));
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:sqr")), ByteBuffer.wrap(bytes(Integer.toString(i * i)))));
            client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations);
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row)));

            // sleep to force later timestamp
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // no-op
            }

            mutations.clear();
            m = new Mutation();
            m.column = ByteBuffer.wrap(bytes("entry:num"));
            m.value= ByteBuffer.wrap(bytes("-999"));
            mutations.add(m);
            m = new Mutation();
            m.column = ByteBuffer.wrap(bytes("entry:sqr"));
            m.isDelete = true;
            client.mutateRowTs(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations, 1); // shouldn't override latest
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row)));

            List<TCell> versions = client.getVer(ByteBuffer.wrap(t), ByteBuffer.wrap(row), ByteBuffer.wrap(bytes("entry:num")), 10);
            printVersions(ByteBuffer.wrap(row), versions);
            if (versions.isEmpty()) {
                System.out.println("FATAL: wrong # of versions");
                System.exit(-1);
            }

            
            List<TCell> result = client.get(ByteBuffer.wrap(t), ByteBuffer.wrap(row), ByteBuffer.wrap(bytes("entry:foo")));
            if (result.isEmpty() == false) {
                System.out.println("FATAL: shouldn't get here");
                System.exit(-1);
            }

            System.out.println("");
        }

        // scan all rows/columnNames

        columnNames.clear();
        for (ColumnDescriptor col2 : client.getColumnDescriptors(ByteBuffer.wrap(t)).values()) {
            System.out.println("column with name: " + new String(col2.name.array()));
            System.out.println(col2.toString());

            columnNames.add(col2.name);
        }

        System.out.println("Starting scanner...");
        scanner = client.scannerOpenWithStop(ByteBuffer.wrap(t), ByteBuffer.wrap(bytes("00020")), ByteBuffer.wrap(bytes("00040")),
                columnNames);

        while (true) {
            List<TRowResult> entry = client.scannerGet(scanner);
            if (entry.isEmpty()) {
                System.out.println("Scanner finished");
                break;
            }
            printRow(entry);
        }

        transport.close();
    }

    private final void printVersions(ByteBuffer row, List<TCell> versions) {
        StringBuilder rowStr = new StringBuilder();
        for (TCell cell : versions) {
            rowStr.append(utf8(cell.value.array()));
            rowStr.append("; ");
        }
        System.out.println("row: " + utf8(row.array()) + ", values: " + rowStr);
    }

    private final void printRow(TRowResult rowResult) {
        // copy values into a TreeMap to get them in sorted order

        TreeMap<String, TCell> sorted = new TreeMap<String, TCell>();
        for (Map.Entry<ByteBuffer, TCell> column : rowResult.columns.entrySet()) {
            sorted.put(utf8(column.getKey().array()), column.getValue());
        }

        StringBuilder rowStr = new StringBuilder();
        for (SortedMap.Entry<String, TCell> entry : sorted.entrySet()) {
            rowStr.append(entry.getKey());
            rowStr.append(" => ");
            rowStr.append(utf8(entry.getValue().value.array()));
            rowStr.append("; ");
        }
        System.out.println("row: " + utf8(rowResult.row.array()) + ", cols: " + rowStr);
    }

    private void printRow(List<TRowResult> rows) {
        for (TRowResult rowResult : rows) {
            printRow(rowResult);
        }
    }
}
