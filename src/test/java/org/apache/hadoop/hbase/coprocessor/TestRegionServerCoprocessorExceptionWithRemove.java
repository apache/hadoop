/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests unhandled exceptions thrown by coprocessors running on regionserver.
 * Expected result is that the master will remove the buggy coprocessor from
 * its set of coprocessors and throw a org.apache.hadoop.hbase.DoNotRetryIOException
 * back to the client.
 * (HBASE-4014).
 */
public class TestRegionServerCoprocessorExceptionWithRemove {
  public static class BuggyRegionObserver extends SimpleRegionObserver {
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
                       final Map<byte[], List<KeyValue>> familyMap,
                       final boolean writeToWAL) {
      String tableName =
          c.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
      if (tableName.equals("observed_table")) {
        Integer i = null;
        i = i + 1;
      }
    }
  }

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ZooKeeperWatcher zkw = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        BuggyRegionObserver.class.getName());
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout=30000)
  public void testExceptionFromCoprocessorDuringPut()
      throws IOException {
    // Set watches on the zookeeper nodes for all of the regionservers in the
    // cluster. When we try to write to TEST_TABLE, the buggy coprocessor will
    // cause a NullPointerException, which will cause the regionserver (which
    // hosts the region we attempted to write to) to abort. In turn, this will
    // cause the nodeDeleted() method of the DeadRegionServer tracker to
    // execute, which will set the rsZKNodeDeleted flag to true, which will
    // pass this test.

    byte[] TEST_TABLE = Bytes.toBytes("observed_table");
    byte[] TEST_FAMILY = Bytes.toBytes("aaa");

    HTable table = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    TEST_UTIL.createMultiRegions(table, TEST_FAMILY);
    // Note which regionServer that should survive the buggy coprocessor's
    // prePut().
    HRegionServer regionServer =
        TEST_UTIL.getRSForFirstRegionInTable(TEST_TABLE);

    // same logic as {@link TestMasterCoprocessorExceptionWithRemove},
    // but exception will be RetriesExhaustedWithDetailException rather
    // than DoNotRetryIOException. The latter exception is what the RegionServer
    // will have actually thrown, but the client will wrap this in a
    // RetriesExhaustedWithDetailException.
    // We will verify that "DoNotRetryIOException" appears in the text of the
    // the exception's detailMessage.
    boolean threwDNRE = false;
    try {
      final byte[] ROW = Bytes.toBytes("aaa");
      Put put = new Put(ROW);
      put.add(TEST_FAMILY, ROW, ROW);
      table.put(put);
    } catch (RetriesExhaustedWithDetailsException e) {
      // below, could call instead :
      // startsWith("Failed 1 action: DoNotRetryIOException.")
      // But that might be too brittle if client-side
      // DoNotRetryIOException-handler changes its message.
      assertTrue(e.getMessage().contains("DoNotRetryIOException"));
      threwDNRE = true;
    } finally {
      assertTrue(threwDNRE);
    }

    // Wait 3 seconds for the regionserver to abort: expected result is that
    // it will survive and not abort.
    for (int i = 0; i < 3; i++) {
      assertFalse(regionServer.isAborted());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        fail("InterruptedException while waiting for regionserver " +
            "zk node to be deleted.");
      }
    }
  }
}
