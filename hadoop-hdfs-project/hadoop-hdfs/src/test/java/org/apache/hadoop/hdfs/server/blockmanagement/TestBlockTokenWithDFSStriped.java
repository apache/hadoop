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
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.junit.Test;

import java.io.IOException;

public class TestBlockTokenWithDFSStriped extends TestBlockTokenWithDFS {

  private final static int dataBlocks = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private final static int parityBlocks = StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private final static int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final static int stripesPerBlock = 4;
  private final static int numDNs = dataBlocks + parityBlocks + 2;
  private MiniDFSCluster cluster;
  private Configuration conf;

  {
    BLOCK_SIZE = cellSize * stripesPerBlock;
    FILE_SIZE =  BLOCK_SIZE * dataBlocks * 3;
  }

  private Configuration getConf() {
    Configuration conf = super.getConf(numDNs);
    conf.setInt("io.bytes.per.checksum", cellSize);
    return conf;
  }

  @Test
  @Override
  public void testRead() throws Exception {
    conf = getConf();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient()
        .setErasureCodingPolicy("/", null);
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
