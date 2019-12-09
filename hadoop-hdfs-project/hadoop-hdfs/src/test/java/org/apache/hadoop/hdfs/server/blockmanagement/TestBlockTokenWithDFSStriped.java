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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.net.ServerSocketUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;

public class TestBlockTokenWithDFSStriped extends TestBlockTokenWithDFS {
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripesPerBlock = 4;
  private final int numDNs = dataBlocks + parityBlocks + 2;
  private MiniDFSCluster cluster;
  private Configuration conf;

  {
    BLOCK_SIZE = cellSize * stripesPerBlock;
    FILE_SIZE =  BLOCK_SIZE * dataBlocks * 3;
  }

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  private Configuration getConf() {
    Configuration conf = super.getConf(numDNs);
    conf.setInt("io.bytes.per.checksum", cellSize);
    return conf;
  }

  @Test
  @Override
  public void testRead() throws Exception {
    conf = getConf();

    /*
     * prefer non-ephemeral port to avoid conflict with tests using
     * ephemeral ports on MiniDFSCluster#restartDataNode(true).
     */
    Configuration[] overlays = new Configuration[numDNs];
    for (int i = 0; i < overlays.length; i++) {
      int offset = i * 10;
      Configuration c = new Configuration();
      c.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:"
          + ServerSocketUtil.getPort(19866 + offset, 100));
      c.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:"
          + ServerSocketUtil.getPort(19867 + offset, 100));
      overlays[i] = c;
    }

    cluster = new MiniDFSCluster.Builder(conf)
        .nameNodePort(ServerSocketUtil.getPort(18020, 100))
        .nameNodeHttpPort(ServerSocketUtil.getPort(19870, 100))
        .numDataNodes(numDNs)
        .build();
    cluster.getFileSystem().enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/",
        StripedFileTestUtil.getDefaultECPolicy().getName());
    try {
      cluster.waitActive();
      doTestRead(conf, cluster, true);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * tested at {@link org.apache.hadoop.hdfs.TestDFSStripedOutputStreamWithFailure#testBlockTokenExpired()}
   */
  @Test
  @Override
  public void testWrite(){
  }

  @Test
  @Override
  public void testAppend() throws Exception {
    //TODO: support Append for striped file
  }

  @Test
  @Override
  public void testEnd2End() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    new TestBalancer().integrationTestWithStripedFile(conf);
  }

  @Override
  protected void tryRead(final Configuration conf, LocatedBlock lblock,
                         boolean shouldSucceed) {
    LocatedStripedBlock lsb = (LocatedStripedBlock) lblock;
    LocatedBlock[] internalBlocks = StripedBlockUtil.parseStripedBlockGroup
        (lsb, cellSize, dataBlocks, parityBlocks);
    for (LocatedBlock internalBlock : internalBlocks) {
      super.tryRead(conf, internalBlock, shouldSucceed);
    }
  }

  @Override
  protected boolean isBlockTokenExpired(LocatedBlock lb) throws IOException {
    LocatedStripedBlock lsb = (LocatedStripedBlock) lb;
    LocatedBlock[] internalBlocks = StripedBlockUtil.parseStripedBlockGroup
        (lsb, cellSize, dataBlocks, parityBlocks);
    for (LocatedBlock internalBlock : internalBlocks) {
      if(super.isBlockTokenExpired(internalBlock)){
        return true;
      }
    }
    return false;
  }
}
