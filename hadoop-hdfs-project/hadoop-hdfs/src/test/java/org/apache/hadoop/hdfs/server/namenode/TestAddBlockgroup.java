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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestAddBlockgroup {

  public static final Log LOG = LogFactory.getLog(TestAddBlockgroup.class);

  private final short GROUP_SIZE = HdfsConstants.NUM_DATA_BLOCKS +
      HdfsConstants.NUM_PARITY_BLOCKS;
  private final short NUM_DATANODES = GROUP_SIZE;

  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 3;

  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
    cluster.getFileSystem().setStoragePolicy(new Path("/"),
        HdfsConstants.EC_STORAGE_POLICY_NAME);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAddBlockGroup() throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();

    final Path file1 = new Path("/file1");
    DFSTestUtil.createFile(fs, file1, BLOCKSIZE * 2, REPLICATION, 0L);
    INodeFile file1Node = fsdir.getINode4Write(file1.toString()).asFile();
    BlockInfo[] file1Blocks = file1Node.getBlocks();
    assertEquals(2, file1Blocks.length);
    assertEquals(GROUP_SIZE, file1Blocks[0].numNodes());
    assertEquals(HdfsConstants.MAX_BLOCKS_IN_GROUP,
        file1Blocks[1].getBlockId() - file1Blocks[0].getBlockId());
  }
}
