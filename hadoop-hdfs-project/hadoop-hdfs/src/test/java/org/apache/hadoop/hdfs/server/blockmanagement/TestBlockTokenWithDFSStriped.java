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
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestBlockTokenWithDFSStriped extends TestBlockTokenWithDFS {

  private final static int dataBlocks = HdfsConstants.NUM_DATA_BLOCKS;
  private final static int parityBlocks = HdfsConstants.NUM_PARITY_BLOCKS;
  private final static int cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private final static int stripesPerBlock = 4;
  private final static int numDNs = dataBlocks + parityBlocks + 2;
  private static MiniDFSCluster cluster;
  private static Configuration conf;

  {
    BLOCK_SIZE = cellSize * stripesPerBlock;
    FILE_SIZE =  BLOCK_SIZE * dataBlocks * 3;
  }

  @Before
  public void setup() throws IOException {
    conf = getConf();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient()
        .createErasureCodingZone("/", null, cellSize);
    cluster.waitActive();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private Configuration getConf() {
    Configuration conf = super.getConf(numDNs);
    conf.setInt("io.bytes.per.checksum", cellSize);
    return conf;
  }

  @Test
  @Override
  public void testRead() throws Exception {
    //TODO: DFSStripedInputStream handles token expiration
//    doTestRead(conf, cluster, true);
  }

  @Test
  @Override
  public void testWrite() throws Exception {
    //TODO: DFSStripedOutputStream handles token expiration
  }

  @Test
  @Override
  public void testAppend() throws Exception {
    //TODO: support Append for striped file
  }

  @Test
  @Override
  public void testEnd2End() throws Exception {
    //TODO: DFSStripedOutputStream handles token expiration
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
