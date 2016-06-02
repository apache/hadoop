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
package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * This test serves a prototype to demo the idea proposed so far. It creates two
 * files using the same data, one is in replica mode, the other is in stripped
 * layout. For simple, it assumes 6 data blocks in both files and the block size
 * are the same.
 */
public class TestFileChecksum {
  public static final Log LOG = LogFactory.getLog(TestFileChecksum.class);

  private int dataBlocks = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private int parityBlocks = StripedFileTestUtil.NUM_PARITY_BLOCKS;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private DFSClient client;

  private int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private int stripesPerBlock = 6;
  private int blockSize = cellSize * stripesPerBlock;
  private int numBlockGroups = 10;
  private int stripSize = cellSize * dataBlocks;
  private int blockGroupSize = stripesPerBlock * stripSize;
  private int fileSize = numBlockGroups * blockGroupSize;

  private String ecDir = "/striped";
  private String stripedFile1 = ecDir + "/stripedFileChecksum1";
  private String stripedFile2 = ecDir + "/stripedFileChecksum2";
  private String replicatedFile = "/replicatedFileChecksum";

  @Before
  public void setup() throws IOException {
    int numDNs = dataBlocks + parityBlocks + 2;
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    Path ecPath = new Path(ecDir);
    cluster.getFileSystem().mkdir(ecPath, FsPermission.getDirDefault());
    cluster.getFileSystem().getClient().setErasureCodingPolicy(ecDir, null);
    fs = cluster.getFileSystem();
    client = fs.getClient();

    prepareTestFiles();

    getDataNodeToKill(stripedFile1);
    getDataNodeToKill(replicatedFile);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testStripedFileChecksum1() throws Exception {
    int length = 0;
    testStripedFileChecksum(length, length + 10);
  }

  @Test
  public void testStripedFileChecksum2() throws Exception {
    int length = stripSize - 1;
    testStripedFileChecksum(length, length - 10);
  }

  @Test
  public void testStripedFileChecksum3() throws Exception {
    int length = stripSize;
    testStripedFileChecksum(length, length - 10);
  }

  @Test
  public void testStripedFileChecksum4() throws Exception {
    int length = stripSize + cellSize * 2;
    testStripedFileChecksum(length, length - 10);
  }

  @Test
  public void testStripedFileChecksum5() throws Exception {
    int length = blockGroupSize;
    testStripedFileChecksum(length, length - 10);
  }

  @Test
  public void testStripedFileChecksum6() throws Exception {
    int length = blockGroupSize + blockSize;
    testStripedFileChecksum(length, length - 10);
  }

  @Test
  public void testStripedFileChecksum7() throws Exception {
    int length = -1; // whole file
    testStripedFileChecksum(length, fileSize);
  }

  void testStripedFileChecksum(int range1, int range2) throws Exception {
    FileChecksum stripedFileChecksum1 = getFileChecksum(stripedFile1,
        range1, false);
    FileChecksum stripedFileChecksum2 = getFileChecksum(stripedFile2,
        range1, false);
    FileChecksum stripedFileChecksum3 = getFileChecksum(stripedFile2,
        range2, false);

    LOG.info("stripedFileChecksum1:" + stripedFileChecksum1);
    LOG.info("stripedFileChecksum2:" + stripedFileChecksum2);
    LOG.info("stripedFileChecksum3:" + stripedFileChecksum3);

    Assert.assertTrue(stripedFileChecksum1.equals(stripedFileChecksum2));
    if (range1 >=0 && range1 != range2) {
      Assert.assertFalse(stripedFileChecksum1.equals(stripedFileChecksum3));
    }
  }

  @Test
  public void testStripedAndReplicatedFileChecksum() throws Exception {
    FileChecksum stripedFileChecksum1 = getFileChecksum(stripedFile1,
        10, false);
    FileChecksum replicatedFileChecksum = getFileChecksum(replicatedFile,
        10, false);

    Assert.assertFalse(stripedFileChecksum1.equals(replicatedFileChecksum));
  }

  @Test
  public void testStripedFileChecksumWithMissedDataBlocks1() throws Exception {
    FileChecksum stripedFileChecksum1 = getFileChecksum(stripedFile1, fileSize,
        false);
    FileChecksum stripedFileChecksumRecon = getFileChecksum(stripedFile1,
        fileSize, true);

    LOG.info("stripedFileChecksum1:" + stripedFileChecksum1);
    LOG.info("stripedFileChecksumRecon:" + stripedFileChecksumRecon);

    Assert.assertTrue("Checksum mismatches!",
        stripedFileChecksum1.equals(stripedFileChecksumRecon));
  }

  @Test
  public void testStripedFileChecksumWithMissedDataBlocks2() throws Exception {
    FileChecksum stripedFileChecksum1 = getFileChecksum(stripedFile1, -1,
        false);
    FileChecksum stripedFileChecksum2 = getFileChecksum(stripedFile2, -1,
        false);
    FileChecksum stripedFileChecksum2Recon = getFileChecksum(stripedFile2, -1,
        true);

    LOG.info("stripedFileChecksum1:" + stripedFileChecksum1);
    LOG.info("stripedFileChecksum2:" + stripedFileChecksum1);
    LOG.info("stripedFileChecksum2Recon:" + stripedFileChecksum2Recon);

    Assert.assertTrue("Checksum mismatches!",
        stripedFileChecksum1.equals(stripedFileChecksum2));
    Assert.assertTrue("Checksum mismatches!",
        stripedFileChecksum1.equals(stripedFileChecksum2Recon));
    Assert.assertTrue("Checksum mismatches!",
        stripedFileChecksum2.equals(stripedFileChecksum2Recon));
  }

  private FileChecksum getFileChecksum(String filePath, int range,
                                       boolean killDn) throws Exception {
    int dnIdxToDie = -1;
    if (killDn) {
      dnIdxToDie = getDataNodeToKill(filePath);
      DataNode dnToDie = cluster.getDataNodes().get(dnIdxToDie);
      shutdownDataNode(dnToDie);
    }

    Path testPath = new Path(filePath);
    FileChecksum fc;

    if (range >= 0) {
      fc = fs.getFileChecksum(testPath, range);
    } else {
      fc = fs.getFileChecksum(testPath);
    }

    if (dnIdxToDie != -1) {
      cluster.restartDataNode(dnIdxToDie, true);
    }

    return fc;
  }

  void prepareTestFiles() throws IOException {
    byte[] fileData = StripedFileTestUtil.generateBytes(fileSize);

    String[] filePaths = new String[] {
        stripedFile1, stripedFile2, replicatedFile
    };

    for (String filePath : filePaths) {
      Path testPath = new Path(filePath);
      DFSTestUtil.writeFile(fs, testPath, fileData);
    }
  }

  void shutdownDataNode(DataNode dataNode) throws IOException {
    /*
     * Kill the datanode which contains one replica
     * We need to make sure it dead in namenode: clear its update time and
     * trigger NN to check heartbeat.
     */
    dataNode.shutdown();
    cluster.setDataNodeDead(dataNode.getDatanodeId());
  }

  /**
   * Determine the datanode that hosts the first block of the file. For simple
   * this just returns the first datanode as it's firstly tried.
   */
  int getDataNodeToKill(String filePath) throws IOException {
    LocatedBlocks locatedBlocks = client.getLocatedBlocks(filePath, 0);

    LocatedBlock locatedBlock = locatedBlocks.get(0);
    DatanodeInfo[] datanodes = locatedBlock.getLocations();
    DatanodeInfo chosenDn = datanodes[0];

    int idx = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getInfoPort() == chosenDn.getInfoPort()) {
        return idx;
      }
      idx++;
    }

    return -1;
  }
}
