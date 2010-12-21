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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Priority;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRegionObserverInterface {
  static final Log LOG = LogFactory.getLog(TestRegionObserverInterface.class);
  static final String DIR = "test/build/data/TestRegionObserver/";

  public static final byte[] TEST_TABLE = Bytes.toBytes("TestTable");
  public static final byte[] TEST_TABLE_2 = Bytes.toBytes("TestTable2");
  public static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  public static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");

  public final static byte[] A = Bytes.toBytes("a");
  public final static byte[] B = Bytes.toBytes("b");
  public final static byte[] C = Bytes.toBytes("c");
  public final static byte[] ROW = Bytes.toBytes("testrow");
  public final static byte[] ROW1 = Bytes.toBytes("testrow1");
  public final static byte[] ROW2 = Bytes.toBytes("testrow2");

  private static final int ROWSIZE = 20;
  private static byte [][] ROWS = makeN(ROW, ROWSIZE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = util.getConfiguration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver");

    util.startMiniCluster(2);
    cluster = util.getMiniHBaseCluster();

    HTable table = util.createTable(TEST_TABLE_2, TEST_FAMILY);

    for(int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }

    // sleep here is an ugly hack to allow region transitions to finish
    Thread.sleep(5000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  HRegion initHRegion (byte [] tableName, String callingMethod,
      Configuration conf, Class<?> implClass, byte [] ... families)
      throws IOException{
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    // this following piece is a hack. currently a coprocessorHost
    // is secretly loaded at OpenRegionHandler. we don't really
    // start a region server here, so just manually create cphost
    // and set it to region.
    HRegion r = HRegion.createHRegion(info, path, conf);
    RegionCoprocessorHost host = new RegionCoprocessorHost(r, null, conf);
    r.setCoprocessorHost(host);
    host.load(implClass, Priority.USER);
    return r;
  }

  @Test
  public void testRegionObserver() throws IOException {
    byte[] TABLE = Bytes.toBytes(getClass().getName());
    byte[][] FAMILIES = new byte[][] { A, B, C } ;

    Put put = new Put(ROW);
    put.add(A, A, A);
    put.add(B, B, B);
    put.add(C, C, C);

    Get get = new Get(ROW);
    get.addColumn(A, A);
    get.addColumn(B, B);
    get.addColumn(C, C);

    Delete delete = new Delete(ROW);
    delete.deleteColumn(A, A);
    delete.deleteColumn(B, B);
    delete.deleteColumn(C, C);

    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      for (HRegionInfo r : t.getRegionServer().getOnlineRegions()) {
        if (!Arrays.equals(r.getTableDesc().getName(), TEST_TABLE)) {
          continue;
        }
        RegionCoprocessorHost cph = t.getRegionServer().getOnlineRegion(r.getRegionName()).
          getCoprocessorHost();
        Coprocessor c = cph.findCoprocessor(SimpleRegionObserver.class.getName());
        assertNotNull(c);
        assertTrue(((SimpleRegionObserver)c).hadPreGet());
        assertTrue(((SimpleRegionObserver)c).hadPostGet());
        assertTrue(((SimpleRegionObserver)c).hadPrePut());
        assertTrue(((SimpleRegionObserver)c).hadPostPut());
        assertTrue(((SimpleRegionObserver)c).hadDelete());
      }
    }
  }

  // TODO: add tests for other methods which need to be tested
  // at region servers.

  @Test
  public void testIncrementHook() throws IOException {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE_2);

    Increment inc = new Increment(Bytes.toBytes(0));
    inc.addColumn(TEST_FAMILY, TEST_QUALIFIER, 1);

    table.increment(inc);

    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      for (HRegionInfo r : t.getRegionServer().getOnlineRegions()) {
        if (!Arrays.equals(r.getTableDesc().getName(), TEST_TABLE_2)) {
          continue;
        }
        RegionCoprocessorHost cph = t.getRegionServer().getOnlineRegion(r.getRegionName()).
          getCoprocessorHost();
        Coprocessor c = cph.findCoprocessor(SimpleRegionObserver.class.getName());
        assertTrue(((SimpleRegionObserver)c).hadPreIncrement());
        assertTrue(((SimpleRegionObserver)c).hadPostIncrement());
      }
    }
  }

  private static byte [][] makeN(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }
}

