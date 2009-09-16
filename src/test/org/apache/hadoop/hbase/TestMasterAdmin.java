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
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/** tests administrative functions */
public class TestMasterAdmin extends HBaseClusterTestCase {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  private static final byte [] FAMILY_NAME = Bytes.toBytes("col1");
  private static HTableDescriptor testDesc;
  static {
    testDesc = new HTableDescriptor("testadmin1");
    testDesc.addFamily(new HColumnDescriptor(FAMILY_NAME));
  }
  
  private HBaseAdmin admin;

  /** constructor */
  public TestMasterAdmin() {
    super();
    admin = null;

    // Make the thread wake frequency a little slower so other threads
    // can run
    conf.setInt("hbase.server.thread.wakefrequency", 2000);
  }
  
  /** @throws Exception */
  public void testMasterAdmin() throws Exception {
    admin = new HBaseAdmin(conf);
    // Add test that exception is thrown if descriptor is without a table name.
    // HADOOP-2156.
    boolean exception = false;
    try {
      admin.createTable(new HTableDescriptor());
    } catch (IllegalArgumentException e) {
      exception = true;
    }
    assertTrue(exception);
    admin.createTable(testDesc);
    LOG.info("Table " + testDesc.getNameAsString() + " created");
    admin.disableTable(testDesc.getName());
    LOG.info("Table " + testDesc.getNameAsString() + " disabled");
    try {
      @SuppressWarnings("unused")
      HTable table = new HTable(conf, testDesc.getName());
    } catch (org.apache.hadoop.hbase.client.RegionOfflineException e) {
      // Expected
    }

    admin.addColumn(testDesc.getName(), new HColumnDescriptor("col2"));
    admin.enableTable(testDesc.getName());
    try {
      admin.deleteColumn(testDesc.getName(), Bytes.toBytes("col2"));
    } catch(TableNotDisabledException e) {
      // Expected
    }

    admin.disableTable(testDesc.getName());
    admin.deleteColumn(testDesc.getName(), Bytes.toBytes("col2"));
    admin.deleteTable(testDesc.getName());
  }
}
