/**
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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

/**
 * Disabling is tricky. This class tests how the Master behaves during those
 */
public class TestMasterWithDisabling {

  private static final Log LOG = LogFactory.getLog(TestMasterWithDisabling.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TABLENAME = Bytes.toBytes("disabling");
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testDisableBetweenSplit() throws IOException {
    // Table that splits like crazy
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    htd.setMaxFileSize(1024);
    htd.setMemStoreFlushSize(1024);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILYNAME);
    htd.addFamily(hcd);
    TEST_UTIL.getHBaseAdmin().createTable(htd);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    HBase2515Listener list = new HBase2515Listener(TEST_UTIL.getHBaseAdmin());
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    m.getRegionServerOperationQueue().
      registerRegionServerOperationListener(list);
    try {
      TEST_UTIL.loadTable(t, FAMILYNAME);
    } catch (IOException ex) {
      // We disable the table during a split, we will end up here
      LOG.info("Expected", ex);
    }
    // Check that there's no region in flight, HBASE-2515
    assertEquals(0,cluster.getMaster().
        getClusterStatus().getRegionsInTransition().size());
  }

  /**
   * Simple listener that simulates a very long processing of a split. When
   * we catch it, we first disable the table then let the processing go forward
   */
  static class HBase2515Listener implements RegionServerOperationListener {
    HBaseAdmin admin;

    public HBase2515Listener(HBaseAdmin admin) {
      this.admin = admin;
    }

    @Override
    public boolean process(HServerInfo serverInfo, HMsg incomingMsg) {
      if (!incomingMsg.isType(HMsg.Type.MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS)) {
        return true;
      }
      try {
        LOG.info("Disabling table");
        admin.disableTable(TABLENAME);
      } catch (IOException e) {
        LOG.warn(e);
        fail("Disable should always work");
      }
      return true;
    }

    @Override
    public boolean process(RegionServerOperation op) throws IOException {
      return true;
    }

    @Override
    public void processed(RegionServerOperation op) {
    }
  }

}
