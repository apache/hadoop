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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.blockSize;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.numDNs;

/**
 * Test reading a striped file when some of its blocks are missing (not included
 * in the block locations returned by the NameNode).
 */
public class TestReadStripedFileWithMissingBlocks {
  public static final Log LOG = LogFactory
      .getLog(TestReadStripedFileWithMissingBlocks.class);
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf = new HdfsConfiguration();
  private final short dataBlocks = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private final int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final int fileLength = blockSize * dataBlocks + 123;

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/", null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReadFileWithMissingBlocks1() throws Exception {
    readFileWithMissingBlocks(new Path("/foo"), fileLength, 1, 0);
  }

  @Test
  public void testReadFileWithMissingBlocks2() throws Exception {
    readFileWithMissingBlocks(new Path("/foo"), fileLength, 1, 1);
  }

  @Test
  public void testReadFileWithMissingBlocks3() throws Exception {
    readFileWithMissingBlocks(new Path("/foo"), fileLength, 1, 2);
  }

  @Test
  public void testReadFileWithMissingBlocks4() throws Exception {
    readFileWithMissingBlocks(new Path("/foo"), fileLength, 2, 0);
  }

  @Test
  public void testReadFileWithMissingBlocks5() throws Exception {
    readFileWithMissingBlocks(new Path("/foo"), fileLength, 2, 1);
  }

  @Test
  public void testReadFileWithMissingBlocks6() throws Exception {
    readFileWithMissingBlocks(new Path("/foo"), fileLength, 3, 0);
  }

  private void readFileWithMissingBlocks(Path srcPath, int fileLength,
      int missingDataNum, int missingParityNum)
      throws Exception {
    LOG.info("readFileWithMissingBlocks: (" + missingDataNum + ","
        + missingParityNum + ")");
    final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
    DFSTestUtil.writeFile(fs, srcPath, new String(expected));
    StripedFileTestUtil.waitBlockGroupsReported(fs, srcPath.toUri().getPath());
    StripedFileTestUtil.verifyLength(fs, srcPath, fileLength);
    int dataBlocks = (fileLength - 1) / cellSize + 1;
    BlockLocation[] locs = fs.getFileBlockLocations(srcPath, 0, cellSize);

    int[] missingDataNodes = new int[missingDataNum + missingParityNum];
    for (int i = 0; i < missingDataNum; i++) {
      missingDataNodes[i] = i;
    }
    for (int i = 0; i < missingParityNum; i++) {
      missingDataNodes[i + missingDataNum] = i +
          Math.min(StripedFileTestUtil.NUM_DATA_BLOCKS, dataBlocks);
    }
    stopDataNodes(locs, missingDataNodes);

    // make sure there are missing block locations
    BlockLocation[] newLocs = fs.getFileBlockLocations(srcPath, 0, cellSize);
    Assert.assertTrue(newLocs[0].getNames().length < locs[0].getNames().length);

    byte[] smallBuf = new byte[1024];
    byte[] largeBuf = new byte[fileLength + 100];
    StripedFileTestUtil.verifySeek(fs, srcPath, fileLength);
    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
        smallBuf);
    StripedFileTestUtil.verifyPread(fs, srcPath, fileLength, expected, largeBuf);

    // delete the file
    fs.delete(srcPath, true);
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
