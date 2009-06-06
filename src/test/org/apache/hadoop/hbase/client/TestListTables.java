/**
 * Copyright 2007 The Apache Software Foundation
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
import java.util.HashSet;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Tests the listTables client API
 */
public class TestListTables extends HBaseClusterTestCase {
  HBaseAdmin admin = null;
  
  private static final HTableDescriptor[] TABLES = {
      new HTableDescriptor("table1"),
      new HTableDescriptor("table2"),
      new HTableDescriptor("table3")
  };
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    admin = new HBaseAdmin(conf);
    HColumnDescriptor family =
      new HColumnDescriptor(HConstants.CATALOG_FAMILY);
    for (int i = 0; i < TABLES.length; i++) {
      TABLES[i].addFamily(family);
      admin.createTable(TABLES[i]);
    }
  }

  /**
   * the test
   * @throws IOException
   */
  public void testListTables() throws IOException {
    HTableDescriptor [] ts = admin.listTables();
    HashSet<HTableDescriptor> result = new HashSet<HTableDescriptor>(ts.length);
    for (int i = 0; i < ts.length; i++) {
      result.add(ts[i]);
    }
    int size = result.size();
    assertEquals(TABLES.length, size);
    for (int i = 0; i < TABLES.length && i < size; i++) {
      assertTrue(result.contains(TABLES[i]));
    }
  }
}