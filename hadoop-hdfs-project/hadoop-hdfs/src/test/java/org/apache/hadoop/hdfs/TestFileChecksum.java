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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;

/**
 * This test serves a prototype to demo the idea proposed so far. It creates two
 * files using the same data, one is in replica mode, the other is in stripped
 * layout. For simple, it assumes 6 data blocks in both files and the block size
 * are the same.
 */
public class TestFileChecksum {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestFileChecksum.class);
  // set as from SystemErasureCodingPolicies.SYS_POLICY1
  private int dataBlocks = 6;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private DFSClient client;

  // set as from SystemErasureCodingPolicies.SYS_POLICY1
  private int cellSize = 1 << 20;
  private int stripesPerBlock = 6;
  private int blockSize = cellSize * stripesPerBlock;
  private int numBlockGroups = 10;
  private int stripSize = cellSize * dataBlocks;
  private int blockGroupSize = stripesPerBlock * stripSize;
  private int fileSize = numBlockGroups * blockGroupSize;
  private int bytesPerCRC;

  private String replicatedFile = "/replicatedFileChecksum";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() throws IOException {
    int numDNs = 3;
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    customizeConf(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    fs = cluster.getFileSystem();
    client = fs.getClient();
    bytesPerCRC = conf.getInt(
        HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    GenericTestUtils.setLogLevel(FileChecksumHelper.LOG, Level.DEBUG);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private byte getByte(long pos) {
    final int mod = 29;
    return (byte) (pos % mod + 1);
  }

  private byte[] generateBytes(int cnt) {
    byte[] bytes = new byte[cnt];
    for (int i = 0; i < cnt; i++) {
      bytes[i] = getByte(i);
    }
    return bytes;
  }

  /**
   * Subclasses may customize the conf to run the full set of tests under
   * different conditions.
   */
  protected void customizeConf(Configuration preparedConf) {
  }

  /**
   * Subclasses may override this method to indicate whether equivalent files
   * in replicated formats with different block sizes are expected to have the
   * same overall FileChecksum.
   */
  protected boolean expectComparableDifferentBlockSizeReplicatedFiles() {
    return false;
  }

  /**
   * Subclasses may override this method to indicate whether checksums are
   * supported for files where different blocks have different bytesPerCRC.
   */
  protected boolean expectSupportForSingleFileMixedBytesPerChecksum() {
    return false;
  }

  @Test(timeout = 90000)
  public void testDifferentBlockSizeReplicatedFileChecksum() throws Exception {
    byte[] fileData = generateBytes(fileSize);
    String replicatedFile1 = "/replicatedFile1";
    String replicatedFile2 = "/replicatedFile2";
    DFSTestUtil.writeFile(
        fs, new Path(replicatedFile1), fileData, blockSize);
    DFSTestUtil.writeFile(
        fs, new Path(replicatedFile2), fileData, blockSize / 2);
    FileChecksum checksum1 = getFileChecksum(replicatedFile1, -1, false);
    FileChecksum checksum2 = getFileChecksum(replicatedFile2, -1, false);

    if (expectComparableDifferentBlockSizeReplicatedFiles()) {
      Assert.assertEquals(checksum1, checksum2);
    } else {
      Assert.assertNotEquals(checksum1, checksum2);
    }
  }

  @Test(timeout = 90000)
  public void testMixedBytesPerChecksum() throws Exception {
    int fileLength = bytesPerCRC * 3;
    byte[] fileData = generateBytes(fileLength);
    String replicatedFile1 = "/replicatedFile1";

    // Split file into two parts.
    byte[] fileDataPart1 = new byte[bytesPerCRC * 2];
    System.arraycopy(fileData, 0, fileDataPart1, 0, fileDataPart1.length);
    byte[] fileDataPart2 = new byte[fileData.length - fileDataPart1.length];
    System.arraycopy(
        fileData, fileDataPart1.length, fileDataPart2, 0, fileDataPart2.length);

    DFSTestUtil.writeFile(fs, new Path(replicatedFile1), fileDataPart1);

    // Modify bytesPerCRC for second part that we append as separate block.
    conf.setInt(
        HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, bytesPerCRC / 2);
    DFSTestUtil.appendFileNewBlock(
        ((DistributedFileSystem) FileSystem.newInstance(conf)),
        new Path(replicatedFile1), fileDataPart2);

    if (expectSupportForSingleFileMixedBytesPerChecksum()) {
      String replicatedFile2 = "/replicatedFile2";
      DFSTestUtil.writeFile(fs, new Path(replicatedFile2), fileData);
      FileChecksum checksum1 = getFileChecksum(replicatedFile1, -1, false);
      FileChecksum checksum2 = getFileChecksum(replicatedFile2, -1, false);
      Assert.assertEquals(checksum1, checksum2);
    } else {
      exception.expect(IOException.class);
      FileChecksum checksum = getFileChecksum(replicatedFile1, -1, false);
    }
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
      cluster.restartDataNode(dnIdxToDie);
    }

    return fc;
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
    DatanodeInfo chosenDn = datanodes[new Random().nextInt(datanodes.length)];

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
