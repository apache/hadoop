/**
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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_final_transistion_failed;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_acquired;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_err;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_resigned;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskBatch;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.OrphanHLogAfterSplitException;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDistributedLogSplitting {
  private static final Log LOG = LogFactory.getLog(TestSplitLogManager.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  // Start a cluster with 2 masters and 3 regionservers
  final int NUM_MASTERS = 2;
  final int NUM_RS = 6;

  MiniHBaseCluster cluster;
  HMaster master;
  Configuration conf;
  HBaseTestingUtility TEST_UTIL;

  private void startCluster(int num_rs) throws Exception{
    ZKSplitLog.Counters.resetCounters();
    LOG.info("Starting cluster");
    conf = HBaseConfiguration.create();
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, num_rs);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
    while (cluster.getLiveRegionServerThreads().size() < num_rs) {
      Threads.sleep(1);
    }
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (timeout=300000)
  public void testThreeRSAbort() throws Exception {
    LOG.info("testThreeRSAbort");
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_ROWS_PER_REGION = 100;

    startCluster(NUM_RS); // NUM_RS=6.

    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
        "distributed log splitting test", null);

    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);
    populateDataInTable(NUM_ROWS_PER_REGION, "family");


    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());
    rsts.get(0).getRegionServer().abort("testing");
    rsts.get(1).getRegionServer().abort("testing");
    rsts.get(2).getRegionServer().abort("testing");

    long start = EnvironmentEdgeManager.currentTimeMillis();
    while (cluster.getLiveRegionServerThreads().size() > (NUM_RS - 3)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    start = EnvironmentEdgeManager.currentTimeMillis();
    while (getAllOnlineRegions(cluster).size() < (NUM_REGIONS_TO_CREATE + 2)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    assertEquals(NUM_REGIONS_TO_CREATE * NUM_ROWS_PER_REGION,
        TEST_UTIL.countRows(ht));
  }

  @Test(expected=OrphanHLogAfterSplitException.class, timeout=300000)
  public void testOrphanLogCreation() throws Exception {
    LOG.info("testOrphanLogCreation");
    startCluster(NUM_RS);
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs = rsts.get(0).getRegionServer();
    Path rootdir = FSUtils.getRootDir(conf);
    final Path logDir = new Path(rootdir,
        HLog.getHLogDirectoryName(hrs.getServerName().toString()));

    installTable(new ZooKeeperWatcher(conf, "table-creation", null),
        "table", "family", 40);

    makeHLog(hrs.getWAL(), hrs.getOnlineRegions(), "table",
        1000, 100);

    new Thread() {
      public void run() {
        while (true) {
          int i = 0;
          try {
            while(ZKSplitLog.Counters.tot_mgr_log_split_batch_start.get() ==
              0) {
              Thread.yield();
            }
            fs.createNewFile(new Path(logDir, "foo" + i++));
          } catch (Exception e) {
            LOG.debug("file creation failed", e);
            return;
          }
        }
      }
    }.start();
    slm.splitLogDistributed(logDir);
    FileStatus[] files = fs.listStatus(logDir);
    if (files != null) {
      for (FileStatus file : files) {
        LOG.debug("file still there " + file.getPath());
      }
    }
  }

  @Test (timeout=300000)
  public void testRecoveredEdits() throws Exception {
    LOG.info("testRecoveredEdits");
    startCluster(NUM_RS);
    final int NUM_LOG_LINES = 1000;
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs = rsts.get(0).getRegionServer();
    Path rootdir = FSUtils.getRootDir(conf);
    final Path logDir = new Path(rootdir,
        HLog.getHLogDirectoryName(hrs.getServerName().toString()));

    installTable(new ZooKeeperWatcher(conf, "table-creation", null),
        "table", "family", 40);
    byte[] table = Bytes.toBytes("table");
    List<HRegionInfo> regions = hrs.getOnlineRegions();
    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaRegion() || region.isRootRegion()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table",
        NUM_LOG_LINES, 100);

    slm.splitLogDistributed(logDir);

    int count = 0;
    for (HRegionInfo hri : regions) {

      Path tdir = HTableDescriptor.getTableDir(rootdir, table);
      Path editsdir =
        HLog.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir,
        hri.getEncodedName()));
      LOG.debug("checking edits dir " + editsdir);
      FileStatus[] files = fs.listStatus(editsdir);
      assertEquals(1, files.length);
      int c = countHLog(files[0].getPath(), fs, conf);
      count += c;
      LOG.info(c + " edits in " + files[0].getPath());
    }
    assertEquals(NUM_LOG_LINES, count);
  }

  @Test (timeout=300000)
  public void testWorkerAbort() throws Exception {
    LOG.info("testWorkerAbort");
    startCluster(1);
    final int NUM_LOG_LINES = 10000;
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    final List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs = rsts.get(0).getRegionServer();
    Path rootdir = FSUtils.getRootDir(conf);
    final Path logDir = new Path(rootdir,
        HLog.getHLogDirectoryName(hrs.getServerName().toString()));

    installTable(new ZooKeeperWatcher(conf, "table-creation", null),
        "table", "family", 40);
    makeHLog(hrs.getWAL(), hrs.getOnlineRegions(), "table",
        NUM_LOG_LINES, 100);

    new Thread() {
      public void run() {
        waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
        for (RegionServerThread rst : rsts) {
          rst.getRegionServer().abort("testing");
        }
      }
    }.start();
    // slm.splitLogDistributed(logDir);
    FileStatus[] logfiles = fs.listStatus(logDir);
    TaskBatch batch = new TaskBatch();
    slm.installTask(logfiles[0].getPath().toString(), batch);
    //waitForCounter but for one of the 2 counters
    long curt = System.currentTimeMillis();
    long endt = curt + 30000;
    while (curt < endt) {
      if ((tot_wkr_task_resigned.get() + tot_wkr_task_err.get() + 
          tot_wkr_final_transistion_failed.get()) == 0) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(1, (tot_wkr_task_resigned.get() + tot_wkr_task_err.get() +
            tot_wkr_final_transistion_failed.get()));
        return;
      }
    }
    assertEquals(1, batch.done);
    // fail("region server completed the split before aborting");
    return;
  }

  HTable installTable(ZooKeeperWatcher zkw, String tname, String fname,
      int nrs ) throws Exception {
    // Create a table with regions
    byte [] table = Bytes.toBytes(tname);
    byte [] family = Bytes.toBytes(fname);
    LOG.info("Creating table with " + nrs + " regions");
    HTable ht = TEST_UTIL.createTable(table, family);
    int numRegions = TEST_UTIL.createMultiRegions(conf, ht, family, nrs);
    assertEquals(nrs, numRegions);
      LOG.info("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    // disable-enable cycle to get rid of table's dead regions left behind
    // by createMultiRegions
    LOG.debug("Disabling table\n");
    TEST_UTIL.getHBaseAdmin().disableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    NavigableSet<String> regions = getAllOnlineRegions(cluster);
    LOG.debug("Verifying only catalog regions are assigned\n");
    if (regions.size() != 2) {
      for (String oregion : regions)
        LOG.debug("Region still online: " + oregion);
    }
    assertEquals(2, regions.size());
    LOG.debug("Enabling table\n");
    TEST_UTIL.getHBaseAdmin().enableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    LOG.debug("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = getAllOnlineRegions(cluster);
    assertEquals(numRegions + 2, regions.size());
    return ht;
  }

  void populateDataInTable(int nrows, String fname) throws Exception {
    byte [] family = Bytes.toBytes(fname);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());

    for (RegionServerThread rst : rsts) {
      HRegionServer hrs = rst.getRegionServer();
      List<HRegionInfo> hris = hrs.getOnlineRegions();
      for (HRegionInfo hri : hris) {
        if (hri.isMetaRegion() || hri.isRootRegion()) {
          continue;
        }
        LOG.debug("adding data to rs = " + rst.getName() +
            " region = "+ hri.getRegionNameAsString());
        HRegion region = hrs.getOnlineRegion(hri.getRegionName());
        assertTrue(region != null);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), family);
      }
    }
  }

  public void makeHLog(HLog log,
      List<HRegionInfo> hris, String tname,
      int num_edits, int edit_size) throws IOException {

    byte[] table = Bytes.toBytes(tname);
    HTableDescriptor htd = new HTableDescriptor(tname);
    byte[] value = new byte[edit_size];
    for (int i = 0; i < edit_size; i++) {
      value[i] = (byte)('a' + (i % 26));
    }
    int n = hris.size();
    int[] counts = new int[n];
    int j = 0;
    if (n > 0) {
      for (int i = 0; i < num_edits; i += 1) {
        WALEdit e = new WALEdit();
        byte [] row = Bytes.toBytes("r" + Integer.toString(i));
        byte [] family = Bytes.toBytes("f");
        byte [] qualifier = Bytes.toBytes("c" + Integer.toString(i));
        e.add(new KeyValue(row, family, qualifier,
            System.currentTimeMillis(), value));
        // LOG.info("Region " + i + ": " + e);
        j++;
        log.append(hris.get(j % n), table, e, System.currentTimeMillis(), htd);
        counts[j % n] += 1;
        // if ((i % 8096) == 0) {
        // log.sync();
        //  }
      }
    }
    log.sync();
    log.close();
    for (int i = 0; i < n; i++) {
      LOG.info("region " + hris.get(i).getRegionNameAsString() +
          " has " + counts[i] + " edits");
    }
    return;
  }

  private int countHLog(Path log, FileSystem fs, Configuration conf)
  throws IOException {
    int count = 0;
    HLog.Reader in = HLog.getReader(fs, log, conf);
    while (in.next() != null) {
      count++;
    }
    return count;
  }

  private void blockUntilNoRIT(ZooKeeperWatcher zkw, HMaster master)
  throws KeeperException, InterruptedException {
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
  }

  private void putData(HRegion region, byte[] startRow, int numRows, byte [] qf,
      byte [] ...families)
  throws IOException {
    for(int i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.add(startRow, Bytes.toBytes(i)));
      for(byte [] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  private NavigableSet<String> getAllOnlineRegions(MiniHBaseCluster cluster)
      throws IOException {
    NavigableSet<String> online = new TreeSet<String>();
    for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      for (HRegionInfo region : rst.getRegionServer().getOnlineRegions()) {
        online.add(region.getRegionNameAsString());
      }
    }
    return online;
  }

  private void waitForCounter(AtomicLong ctr, long oldval, long newval,
      long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.get() == oldval) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, ctr.get());
        return;
      }
    }
    assertTrue(false);
  }
}
