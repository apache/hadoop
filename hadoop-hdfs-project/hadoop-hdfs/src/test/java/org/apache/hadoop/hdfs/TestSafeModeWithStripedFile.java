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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSafeModeWithStripedFile {

  private ErasureCodingPolicy ecPolicy;
  private short dataBlocks;
  private short parityBlocks;
  private int numDNs;
  private int cellSize;
  private int blockSize;

  private MiniDFSCluster cluster;
  private Configuration conf;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  @Before
  public void setup() throws IOException {
    ecPolicy = getEcPolicy();
    dataBlocks = (short) ecPolicy.getNumDataUnits();
    parityBlocks = (short) ecPolicy.getNumParityUnits();
    numDNs = dataBlocks + parityBlocks;
    cellSize = ecPolicy.getCellSize();
    blockSize = cellSize * 2;

    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 100);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().enableErasureCodingPolicy(getEcPolicy().getName());
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/",
        getEcPolicy().getName());
    cluster.waitActive();
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testStripedFile0() throws IOException {
    doTest(cellSize, 1);
  }

  @Test
  public void testStripedFile1() throws IOException {
    int numCell = dataBlocks - 1;
    doTest(cellSize * numCell, numCell);
  }

  /**
   * This util writes a small block group whose size is given by caller.
   * Then write another 2 full stripe blocks.
   * Then shutdown all DNs and start again one by one. and verify the safemode
   * status accordingly.
   *
   * @param smallSize file size of the small block group
   * @param minStorages minimum replicas needed by the block so it can be safe
   */
  private void doTest(int smallSize, int minStorages) throws IOException {
    FileSystem fs = cluster.getFileSystem();
    // add 1 block
    byte[] data = StripedFileTestUtil.generateBytes(smallSize);
    Path smallFilePath = new Path("/testStripedFile_" + smallSize);
    DFSTestUtil.writeFile(fs, smallFilePath, data);

    // If we only have 1 block, NN won't enter safemode in the first place
    // because the threshold is 0 blocks.
    // So we need to add another 2 blocks.
    int bigSize = blockSize * dataBlocks * 2;
    Path bigFilePath = new Path("/testStripedFile_" + bigSize);
    data = StripedFileTestUtil.generateBytes(bigSize);
    DFSTestUtil.writeFile(fs, bigFilePath, data);
    // now we have 3 blocks. NN needs 2 blocks to reach the threshold 0.9 of
    // total blocks 3.

    // stopping all DNs
    List<MiniDFSCluster.DataNodeProperties> dnprops = Lists.newArrayList();
    LocatedBlocks lbs = cluster.getNameNodeRpc()
        .getBlockLocations(smallFilePath.toString(), 0, smallSize);
    DatanodeInfo[] locations = lbs.get(0).getLocations();
    for (DatanodeInfo loc : locations) {
      // keep the DNs that have smallFile in the head of dnprops
      dnprops.add(cluster.stopDataNode(loc.getName()));
    }
    for (int i = 0; i < numDNs - locations.length; i++) {
      dnprops.add(cluster.stopDataNode(0));
    }

    cluster.restartNameNode(0);
    NameNode nn = cluster.getNameNode();
    assertTrue(cluster.getNameNode().isInSafeMode());
    assertEquals(0, NameNodeAdapter.getSafeModeSafeBlocks(nn));

    // the block of smallFile doesn't reach minStorages,
    // so the safe blocks count doesn't increment.
    for (int i = 0; i < minStorages - 1; i++) {
      cluster.restartDataNode(dnprops.remove(0));
      cluster.waitActive();
      cluster.triggerBlockReports();
      assertEquals(0, NameNodeAdapter.getSafeModeSafeBlocks(nn));
    }

    // the block of smallFile reaches minStorages,
    // so the safe blocks count increment.
    cluster.restartDataNode(dnprops.remove(0));
    cluster.waitActive();
    cluster.triggerBlockReports();
    assertEquals(1, NameNodeAdapter.getSafeModeSafeBlocks(nn));

    // the 2 blocks of bigFile need DATA_BLK_NUM storages to be safe
    for (int i = minStorages; i < dataBlocks - 1; i++) {
      cluster.restartDataNode(dnprops.remove(0));
      cluster.waitActive();
      cluster.triggerBlockReports();
      assertTrue(nn.isInSafeMode());
    }

    cluster.restartDataNode(dnprops.remove(0));
    cluster.waitActive();
    cluster.triggerBlockReports();
    assertFalse(nn.isInSafeMode());
  }

}
