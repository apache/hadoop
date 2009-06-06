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

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test gets
 */
public class TestGet extends HBaseClusterTestCase {
  
  private static final byte [] FAMILY = Bytes.toBytes("family");
  
  private static final byte [] ROW = Bytes.toBytes("row");
  
  private static final byte [] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte [] VALUE = Bytes.toBytes("value");
  
  private static final byte [] MISSING_ROW = Bytes.toBytes("missingrow");
  
  private HTableDescriptor desc = null;
  private HTable table = null;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestGet() throws UnsupportedEncodingException {
    super();
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.desc = new HTableDescriptor("testGet");
    desc.addFamily(new HColumnDescriptor(FAMILY));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
  }

  public void testGet_EmptyTable() throws IOException {
    
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertTrue(r.isEmpty());
    
  }
  
  public void testGet_NonExistentRow() throws IOException {
    
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    System.out.println("Row put");
    
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertFalse(r.isEmpty());
    System.out.println("Row retrieved successfully");
    
    get = new Get(MISSING_ROW);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertTrue(r.isEmpty());
    System.out.println("Row missing as it should be");
    
  }
     
}
