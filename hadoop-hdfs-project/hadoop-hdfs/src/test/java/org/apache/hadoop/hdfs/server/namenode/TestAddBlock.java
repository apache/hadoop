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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test AddBlockOp is written and read correctly
 */
public class TestAddBlock {
  private static final short REPLICATION = 3;
  private static final int BLOCKSIZE = 1024;
  
  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
  }
  
  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Test adding new blocks. Restart the NameNode in the test to make sure the
   * AddBlockOp in the editlog is applied correctly. 
   */
  @Test
  public void testAddBlock() throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    
    final Path file1 = new Path("/file1");
    final Path file2 = new Path("/file2");
    final Path file3 = new Path("/file3");
    final Path file4 = new Path("/file4");
    
    DFSTestUtil.createFile(fs, file1, BLOCKSIZE - 1, REPLICATION, 0L);
    DFSTestUtil.createFile(fs, file2, BLOCKSIZE, REPLICATION, 0L);
    DFSTestUtil.createFile(fs, file3, BLOCKSIZE * 2 - 1, REPLICATION, 0L);
    DFSTestUtil.createFile(fs, file4, BLOCKSIZE * 2, REPLICATION, 0L);
    
    // restart NameNode
    cluster.restartNameNode(true);
    FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
    
    // check file1
    INodeFile file1Node = fsdir.getINode4Write(file1.toString()).asFile();
    BlockInfoContiguous[] file1Blocks = file1Node.getBlocks();
    assertEquals(1, file1Blocks.length);
    assertEquals(BLOCKSIZE - 1, file1Blocks[0].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file1Blocks[0].getBlockUCState());
    
    // check file2
    INodeFile file2Node = fsdir.getINode4Write(file2.toString()).asFile();
    BlockInfoContiguous[] file2Blocks = file2Node.getBlocks();
    assertEquals(1, file2Blocks.length);
    assertEquals(BLOCKSIZE, file2Blocks[0].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file2Blocks[0].getBlockUCState());
    
    // check file3
    INodeFile file3Node = fsdir.getINode4Write(file3.toString()).asFile();
    BlockInfoContiguous[] file3Blocks = file3Node.getBlocks();
    assertEquals(2, file3Blocks.length);
    assertEquals(BLOCKSIZE, file3Blocks[0].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file3Blocks[0].getBlockUCState());
    assertEquals(BLOCKSIZE - 1, file3Blocks[1].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file3Blocks[1].getBlockUCState());
    
    // check file4
    INodeFile file4Node = fsdir.getINode4Write(file4.toString()).asFile();
    BlockInfoContiguous[] file4Blocks = file4Node.getBlocks();
    assertEquals(2, file4Blocks.length);
    assertEquals(BLOCKSIZE, file4Blocks[0].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file4Blocks[0].getBlockUCState());
    assertEquals(BLOCKSIZE, file4Blocks[1].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file4Blocks[1].getBlockUCState());
  }
  
  /**
   * Test adding new blocks but without closing the corresponding the file
   */
  @Test
  public void testAddBlockUC() throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    final Path file1 = new Path("/file1");
    DFSTestUtil.createFile(fs, file1, BLOCKSIZE - 1, REPLICATION, 0L);
    
    FSDataOutputStream out = null;
    try {
      // append files without closing the streams
      out = fs.append(file1);
      String appendContent = "appending-content";
      out.writeBytes(appendContent);
      ((DFSOutputStream) out.getWrappedStream()).hsync(
          EnumSet.of(SyncFlag.UPDATE_LENGTH));
      
      // restart NN
      cluster.restartNameNode(true);
      FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
      
      INodeFile fileNode = fsdir.getINode4Write(file1.toString()).asFile();
      BlockInfoContiguous[] fileBlocks = fileNode.getBlocks();
      assertEquals(2, fileBlocks.length);
      assertEquals(BLOCKSIZE, fileBlocks[0].getNumBytes());
      assertEquals(BlockUCState.COMPLETE, fileBlocks[0].getBlockUCState());
      assertEquals(appendContent.length() - 1, fileBlocks[1].getNumBytes());
      assertEquals(BlockUCState.UNDER_CONSTRUCTION,
          fileBlocks[1].getBlockUCState());
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
}
