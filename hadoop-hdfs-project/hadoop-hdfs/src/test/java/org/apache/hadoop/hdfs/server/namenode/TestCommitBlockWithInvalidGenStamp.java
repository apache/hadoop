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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class TestCommitBlockWithInvalidGenStamp {
  private static final int BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster;
  private FSDirectory dir;
  private DistributedFileSystem dfs;

  @Before
  public void setUp() throws IOException {
    final Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

    dir = cluster.getNamesystem().getFSDirectory();
    dfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testCommitWithInvalidGenStamp() throws Exception {
    final Path file = new Path("/file");
    FSDataOutputStream out = null;

    try {
      out = dfs.create(file, (short) 1);
      INodeFile fileNode = dir.getINode4Write(file.toString()).asFile();
      ExtendedBlock previous = null;

      Block newBlock = DFSTestUtil.addBlockToFile(false, cluster.getDataNodes(),
          dfs, cluster.getNamesystem(), file.toString(), fileNode,
          dfs.getClient().getClientName(), previous, 0, 100);
      Block newBlockClone = new Block(newBlock);
      previous = new ExtendedBlock(cluster.getNamesystem().getBlockPoolId(),
          newBlockClone);

      previous.setGenerationStamp(123);
      try{
        dfs.getClient().getNamenode().complete(file.toString(),
            dfs.getClient().getClientName(), previous, fileNode.getId());
        Assert.fail("should throw exception because invalid genStamp");
      } catch (IOException e) {
        Assert.assertTrue(e.toString().contains(
            "Commit block with mismatching GS. NN has " +
            newBlock + ", client submits " + newBlockClone));
      }
      previous = new ExtendedBlock(cluster.getNamesystem().getBlockPoolId(),
          newBlock);
      boolean complete =  dfs.getClient().getNamenode().complete(file.toString(),
      dfs.getClient().getClientName(), previous, fileNode.getId());
      Assert.assertTrue("should complete successfully", complete);
    } finally {
      IOUtils.cleanup(null, out);
    }
  }
}
