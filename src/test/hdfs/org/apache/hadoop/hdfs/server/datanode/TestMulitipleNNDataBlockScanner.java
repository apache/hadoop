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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;


public class TestMulitipleNNDataBlockScanner {
  private static final Log LOG = 
    LogFactory.getLog(TestMulitipleNNDataBlockScanner.class);
  Configuration conf;
  MiniDFSCluster cluster = null;
  String bpids[] = new String[3];
  FileSystem fs[] = new FileSystem[3];
  
  public void setUp(int port) throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 100);
    cluster = new MiniDFSCluster.Builder(conf).numNameNodes(3).nameNodePort(
        port).build();
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
  
  @Test
  public void testDataBlockScanner() throws IOException, InterruptedException {
    setUp(9923);
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
  
  @Test
  public void testBlockScannerAfterRefresh() throws IOException,
      InterruptedException {
    setUp(9933);
    try {
      Configuration conf = new HdfsConfiguration();
      StringBuilder namenodesBuilder = new StringBuilder();

      String bpidToShutdown = cluster.getNamesystem(2).getBlockPoolId();
      for (int i = 0; i < 2; i++) {
        FileSystem fs = cluster.getFileSystem(i);
        namenodesBuilder.append(fs.getUri());
        namenodesBuilder.append(",");
      }

      conf.set(DFSConfigKeys.DFS_FEDERATION_NAMENODES, namenodesBuilder
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

      namenodesBuilder.append(cluster.getFileSystem(2).getUri());
      conf.set(DFSConfigKeys.DFS_FEDERATION_NAMENODES, namenodesBuilder
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
  
  @Test
  public void testBlockScannerAfterRestart() throws IOException,
      InterruptedException {
    setUp(9943);
    try {
      cluster.restartDataNode(0);
      DataNode dn = cluster.getDataNodes().get(0);
      while (!dn.isDatanodeUp()) {
        Thread.sleep(2000);
      }
      for (int i = 0; i < 3; i++) {
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
}
