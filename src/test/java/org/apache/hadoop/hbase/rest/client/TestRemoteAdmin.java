/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.rest.client;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.HBaseRESTTestingUtility;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRemoteAdmin {
  private static final String TABLE_1 = "TestRemoteAdmin_Table_1";
  private static final byte[] COLUMN_1 = Bytes.toBytes("a");
  static final HTableDescriptor DESC_1 =  new HTableDescriptor(TABLE_1);
  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = 
    new HBaseRESTTestingUtility();
  private static RemoteAdmin remoteAdmin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    DESC_1.addFamily(new HColumnDescriptor(COLUMN_1));

    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());

    remoteAdmin = new RemoteAdmin(new Client(
      new Cluster().add("localhost", REST_TEST_UTIL.getServletPort())),
      TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateAnDeleteTable() throws Exception {
    assertFalse(remoteAdmin.isTableAvailable(TABLE_1));
    remoteAdmin.createTable(DESC_1);
    assertTrue(remoteAdmin.isTableAvailable(TABLE_1));
    remoteAdmin.deleteTable(TABLE_1);
    assertFalse(remoteAdmin.isTableAvailable(TABLE_1));
  }
}
