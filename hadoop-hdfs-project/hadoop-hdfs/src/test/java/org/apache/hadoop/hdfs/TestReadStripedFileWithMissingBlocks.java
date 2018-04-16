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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.IOException;

/**
 * Test reading a striped file when some of its blocks are missing (not included
 * in the block locations returned by the NameNode).
 */
public class TestReadStripedFileWithMissingBlocks {
  public static final Log LOG = LogFactory
      .getLog(TestReadStripedFileWithMissingBlocks.class);
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient dfsClient;
  private Configuration conf = new HdfsConfiguration();
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripPerBlock = 4;
  private final int blockSize = stripPerBlock * cellSize;
  private final int blockGroupSize = blockSize * dataBlocks;
  // Starting with two more datanodes, minimum 9 should be up for
  // test to pass.
  private final int numDNs = dataBlocks + parityBlocks + 2;
  private final int fileLength = blockSize * dataBlocks + 123;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().setErasureCodingPolicy(
        "/", ecPolicy.getName());
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    dfsClient = new DFSClient(cluster.getNameNode(0).getNameNodeAddress(),
        conf);
  }

  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testReadFileWithMissingBlocks() throws Exception {
    try {
      setup();
      Path srcPath = new Path("/foo");
      final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
      DFSTestUtil.writeFile(fs, srcPath, new String(expected));
      StripedFileTestUtil
          .waitBlockGroupsReported(fs, srcPath.toUri().getPath());
      StripedFileTestUtil.verifyLength(fs, srcPath, fileLength);

      for (int missingData = 1; missingData <= dataBlocks; missingData++) {
        for (int missingParity = 0; missingParity <=
            parityBlocks - missingData; missingParity++) {
          readFileWithMissingBlocks(srcPath, fileLength, missingData,
              missingParity, expected);
        }
      }
    } finally {
      tearDown();
    }
  }


  private void readFileWithMissingBlocks(Path srcPath, int fileLength,
      int missingDataNum, int missingParityNum, byte[] expected)
      throws Exception {
    LOG.info("readFileWithMissingBlocks: (" + missingDataNum + ","
        + missingParityNum + ")");

    int dataBlocks = (fileLength - 1) / cellSize + 1;
    BlockLocation[] locs = fs.getFileBlockLocations(srcPath, 0, cellSize);

    int[] missingDataNodes = new int[missingDataNum + missingParityNum];
    for (int i = 0; i < missingDataNum; i++) {
      missingDataNodes[i] = i;
    }
    for (int i = 0; i < missingParityNum; i++) {
      missingDataNodes[i + missingDataNum] = i +
          Math.min(ecPolicy.getNumDataUnits(), dataBlocks);
    }
    stopDataNodes(locs, missingDataNodes);

    // make sure there are missing block locations
    BlockLocation[] newLocs = fs.getFileBlockLocations(srcPath, 0, cellSize);
    Assert.assertTrue(
        newLocs[0].getNames().length < locs[0].getNames().length);

    byte[] smallBuf = new byte[1024];
    byte[] largeBuf = new byte[fileLength + 100];
    StripedFileTestUtil.verifySeek(fs, srcPath, fileLength, ecPolicy,
        blockGroupSize);
    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
        smallBuf);
    StripedFileTestUtil
        .verifyPread(fs, srcPath, fileLength, expected, largeBuf);
    restartDeadDataNodes();
  }

  private void restartDeadDataNodes() throws IOException {
    DatanodeInfo[] deadNodes = dfsClient
        .datanodeReport(DatanodeReportType.DEAD);
    for (DatanodeInfo dnInfo : deadNodes) {
      cluster.restartDataNode(dnInfo.getXferAddr());
    }
    cluster.triggerHeartbeats();
  }

  private void stopDataNodes(BlockLocation[] locs, int[] datanodes)
      throws IOException {
    if (locs != null && locs.length > 0) {
      for (int failedDNIdx : datanodes) {
        String name = (locs[0].getNames())[failedDNIdx];
        for (DataNode dn : cluster.getDataNodes()) {
          int port = dn.getXferPort();
          if (name.contains(Integer.toString(port))) {
            dn.shutdown();
            cluster.setDataNodeDead(dn.getDatanodeId());
            LOG.info("stop datanode " + failedDNIdx);
            break;
          }
        }
      }
    }
  }
}
