/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test puts
 */
public class TestPut extends HBaseClusterTestCase {
  private static final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
  private static final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");

  private static final byte [] row1 = Bytes.toBytes("row1");
  private static final byte [] row2 = Bytes.toBytes("row2");
  
  private static final int SMALL_LENGTH = 1;
  private static final int NB_BATCH_ROWS = 10;
  private byte [] value;
  private byte [] smallValue;

  private HTableDescriptor desc = null;
  private HTable table = null;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestPut() throws UnsupportedEncodingException {
    super();
    value = Bytes.toBytes("abcd");
    smallValue = Bytes.toBytes("a");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(CONTENTS_FAMILY));
    desc.addFamily(new HColumnDescriptor(SMALL_FAMILY, 
        HColumnDescriptor.DEFAULT_VERSIONS, 
        HColumnDescriptor.DEFAULT_COMPRESSION,
        HColumnDescriptor.DEFAULT_IN_MEMORY, 
        HColumnDescriptor.DEFAULT_BLOCKCACHE, SMALL_LENGTH, 
        HColumnDescriptor.DEFAULT_TTL, HColumnDescriptor.DEFAULT_BLOOMFILTER));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
  }

  /**
   * @throws IOException
   */
  public void testPut() throws IOException {
    Put put = new Put(row1);
    put.add(CONTENTS_FAMILY, null, value);
    table.put(put);

    put = new Put(row2);
    put.add(CONTENTS_FAMILY, null, value);
    
    assertEquals(put.size(), 1);
    assertEquals(put.getFamilyMap().get(CONTENTS_FAMILY).size(), 1);
    
    KeyValue kv = put.getFamilyMap().get(CONTENTS_FAMILY).get(0);
    
    assertTrue(Bytes.equals(kv.getFamily(), CONTENTS_FAMILY));
    // will it return null or an empty byte array?
    assertTrue(Bytes.equals(kv.getQualifier(), new byte[0]));
    
    assertTrue(Bytes.equals(kv.getValue(), value));
    
    table.put(put);

    Scan scan = new Scan();
    scan.addColumn(CONTENTS_FAMILY, null);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r : scanner) {
      for(KeyValue key : r.sorted()) {
        System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
      }
    }
  }
  
  public void testRowsPut() {
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for(int i = 0; i < NB_BATCH_ROWS; i++) {
      byte [] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    try {
      table.put(rowsUpdate);  
    
      Scan scan = new Scan();
      scan.addFamily(CONTENTS_FAMILY);
      ResultScanner scanner = table.getScanner(scan);
      int nbRows = 0;
      for(@SuppressWarnings("unused") Result row : scanner)
        nbRows++;
      assertEquals(NB_BATCH_ROWS, nbRows);
    } catch (IOException e) {
      fail("This is unexpected : " + e);
    }
  }
  
  public void testRowsPutBufferedOneFlush() {
    table.setAutoFlush(false);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for(int i = 0; i < NB_BATCH_ROWS*10; i++) {
      byte [] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    try {
      table.put(rowsUpdate);  
    
      Scan scan = new Scan();
      scan.addFamily(CONTENTS_FAMILY);
      ResultScanner scanner = table.getScanner(scan);
      int nbRows = 0;
      for(@SuppressWarnings("unused") Result row : scanner)
        nbRows++;
      assertEquals(0, nbRows);  
      scanner.close();
      
      table.flushCommits();
      
      scan = new Scan();
      scan.addFamily(CONTENTS_FAMILY);
      scanner = table.getScanner(scan);
      nbRows = 0;
      for(@SuppressWarnings("unused") Result row : scanner)
        nbRows++;
      assertEquals(NB_BATCH_ROWS*10, nbRows);
    } catch (IOException e) {
      fail("This is unexpected : " + e);
    }
  }
  
  public void testRowsPutBufferedManyManyFlushes() {
    table.setAutoFlush(false);
    table.setWriteBufferSize(10);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for(int i = 0; i < NB_BATCH_ROWS*10; i++) {
      byte [] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    try {
      table.put(rowsUpdate);
      
      table.flushCommits();
      
      Scan scan = new Scan();
      scan.addFamily(CONTENTS_FAMILY);
      ResultScanner scanner = table.getScanner(scan);
      int nbRows = 0;
      for(@SuppressWarnings("unused") Result row : scanner)
        nbRows++;
      assertEquals(NB_BATCH_ROWS*10, nbRows);
    } catch (IOException e) {
      fail("This is unexpected : " + e);
    }
  }
  
  public void testAddKeyValue() throws IOException {
  	byte [] qualifier = Bytes.toBytes("qf1");
    Put put = new Put(row1);
    
    //Adding KeyValue with the same row
    KeyValue kv = new KeyValue(row1, CONTENTS_FAMILY, qualifier, value);
    boolean ok = true;
    try {
    	put.add(kv);
    } catch (IOException e) {
    	ok = false;
    }
    assertEquals(true, ok);
    
    //Adding KeyValue with the different row
    kv = new KeyValue(row2, CONTENTS_FAMILY, qualifier, value);
    ok = false;
    try {
    	put.add(kv);
    } catch (IOException e) {
    	ok = true;
    }
    assertEquals(true, ok);
  }
  
}
