/**
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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import static org.apache.hadoop.hdfs.server.datanode.DataBlockScanner.SLEEP_PERIOD_MS;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.fail;


public class TestMultipleNNDataBlockScanner {
  private static final Log LOG = 
    LogFactory.getLog(TestMultipleNNDataBlockScanner.class);
  Configuration conf;
  MiniDFSCluster cluster = null;
  final String[] bpids = new String[3];
  final FileSystem[] fs = new FileSystem[3];
  
  public void setUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 100);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(3))
        .build();
    for (int i = 0; i < 3; i++) {
      cluster.waitActive(i);
    }
    for (int i = 0; i < 3; i++) {
      bpids[i] = cluster.getNamesystem(i).getBlockPoolId();
    }
    for (int i = 0; i < 3; i++) {
      fs[i] = cluster.getFileSystem(i);
    }
    // Create 2 files on each namenode with 10 blocks each
    for (int i = 0; i < 3; i++) {
      DFSTestUtil.createFile(fs[i], new Path("file1"), 1000, (short) 1, 0);
      DFSTestUtil.createFile(fs[i], new Path("file2"), 1000, (short) 1, 1);
    }
  }
  
  @Test(timeout=120000)
  public void testDataBlockScanner() throws IOException, InterruptedException {
    setUp();
    try {
      DataNode dn = cluster.getDataNodes().get(0);
      for (int i = 0; i < 3; i++) {
        long blocksScanned = 0;
        while (blocksScanned != 20) {
          blocksScanned = dn.blockScanner.getBlocksScannedInLastRun(bpids[i]);
          LOG.info("Waiting for all blocks to be scanned for bpid=" + bpids[i]
              + "; Scanned so far=" + blocksScanned);
          Thread.sleep(5000);
        }
      }

      StringBuilder buffer = new StringBuilder();
      dn.blockScanner.printBlockReport(buffer, false);
      LOG.info("Block Report\n" + buffer.toString());
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test(timeout=120000)
  public void testBlockScannerAfterRefresh() throws IOException,
      InterruptedException {
    setUp();
    try {
      Configuration dnConf = cluster.getDataNodes().get(0).getConf();
      Configuration conf = new HdfsConfiguration(dnConf);
      StringBuilder namenodesBuilder = new StringBuilder();

      String bpidToShutdown = cluster.getNamesystem(2).getBlockPoolId();
      for (int i = 0; i < 2; i++) {
        String nsId = DFSUtil.getNamenodeNameServiceId(cluster
            .getConfiguration(i));
        namenodesBuilder.append(nsId);
        namenodesBuilder.append(",");
      }

      conf.set(DFSConfigKeys.DFS_NAMESERVICES, namenodesBuilder
          .toString());
      DataNode dn = cluster.getDataNodes().get(0);
      dn.refreshNamenodes(conf);

      try {
        while (true) {
          dn.blockScanner.getBlocksScannedInLastRun(bpidToShutdown);
          Thread.sleep(1000);
        }
      } catch (IOException ex) {
        // Expected
        LOG.info(ex.getMessage());
      }

      namenodesBuilder.append(DFSUtil.getNamenodeNameServiceId(cluster
          .getConfiguration(2)));
      conf.set(DFSConfigKeys.DFS_NAMESERVICES, namenodesBuilder
          .toString());
      dn.refreshNamenodes(conf);

      for (int i = 0; i < 3; i++) {
        long blocksScanned = 0;
        while (blocksScanned != 20) {
          blocksScanned = dn.blockScanner.getBlocksScannedInLastRun(bpids[i]);
          LOG.info("Waiting for all blocks to be scanned for bpid=" + bpids[i]
              + "; Scanned so far=" + blocksScanned);
          Thread.sleep(5000);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test(timeout=120000)
  public void testBlockScannerAfterRestart() throws IOException,
      InterruptedException {
    setUp();
    try {
      cluster.restartDataNode(0);
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      for (int i = 0; i < 3; i++) {
        while (!dn.blockScanner.isInitialized(bpids[i])) {
          Thread.sleep(1000);
        }
        long blocksScanned = 0;
        while (blocksScanned != 20) {
          if (dn.blockScanner != null) {
            blocksScanned = dn.blockScanner.getBlocksScannedInLastRun(bpids[i]);
            LOG.info("Waiting for all blocks to be scanned for bpid="
                + bpids[i] + "; Scanned so far=" + blocksScanned);
          }
          Thread.sleep(5000);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test(timeout=120000)
  public void test2NNBlockRescanInterval() throws IOException {
    ((Log4JLogger)BlockPoolSliceScanner.LOG).getLogger().setLevel(Level.ALL);
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(3))
        .build();

    try {
      FileSystem fs = cluster.getFileSystem(1);
      Path file2 = new Path("/test/testBlockScanInterval");
      DFSTestUtil.createFile(fs, file2, 30, (short) 1, 0);

      fs = cluster.getFileSystem(0);
      Path file1 = new Path("/test/testBlockScanInterval");
      DFSTestUtil.createFile(fs, file1, 30, (short) 1, 0);
      for (int i = 0; i < 8; i++) {
        LOG.info("Verifying that the blockscanner scans exactly once");
        waitAndScanBlocks(1, 1);
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * HDFS-3828: DN rescans blocks too frequently
   * 
   * @throws Exception
   */
  @Test(timeout=120000)
  public void testBlockRescanInterval() throws IOException {
    ((Log4JLogger)BlockPoolSliceScanner.LOG).getLogger().setLevel(Level.ALL);
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      FileSystem fs = cluster.getFileSystem();
      Path file1 = new Path("/test/testBlockScanInterval");
      DFSTestUtil.createFile(fs, file1, 30, (short) 1, 0);
      for (int i = 0; i < 4; i++) {
        LOG.info("Verifying that the blockscanner scans exactly once");
        waitAndScanBlocks(1, 1);
      }
    } finally {
      cluster.shutdown();
    }
  }

  void waitAndScanBlocks(long scansLastRun, long scansTotal)
      throws IOException {
    // DataBlockScanner will run for every 5 seconds so we are checking for
    // every 5 seconds
    int n = 5;
    String bpid = cluster.getNamesystem(0).getBlockPoolId();
    DataNode dn = cluster.getDataNodes().get(0);
    long blocksScanned, total;
    do {
      try {
        Thread.sleep(SLEEP_PERIOD_MS);
      } catch (InterruptedException e) {
        fail("Interrupted: " + e);
      }
      blocksScanned = dn.blockScanner.getBlocksScannedInLastRun(bpid);
      total = dn.blockScanner.getTotalScans(bpid);
      LOG.info("bpid = " + bpid + " blocksScanned = " + blocksScanned + " total=" + total);
    } while (n-- > 0 && (blocksScanned != scansLastRun || scansTotal != total));
    Assert.assertEquals(scansTotal, total);
    Assert.assertEquals(scansLastRun, blocksScanned);
  }
}
