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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;

import org.junit.Test;
import org.junit.Assert;

/**
 * This tests pipeline recovery related client protocol works correct or not.
 */
public class TestClientProtocolForPipelineRecovery {
  
  @Test public void testGetNewStamp() throws IOException {
    int numDataNodes = 1;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
    try {
      cluster.waitActive();
      FileSystem fileSys = cluster.getFileSystem();
      NameNode namenode = cluster.getNameNode();

      /* Test writing to finalized replicas */
      Path file = new Path("dataprotocol.dat");    
      DFSTestUtil.createFile(fileSys, file, 1L, (short)numDataNodes, 0L);
      // get the first blockid for the file
      Block firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);

      // test getNewStampAndToken on a finalized block
      try {
        namenode.updateBlockForPipeline(firstBlock, "");
        Assert.fail("Can not get a new GS from a finalized block");
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().contains("is not under Construction"));
      }
      
      // test getNewStampAndToken on a non-existent block
      try {
        long newBlockId = firstBlock.getBlockId() + 1;
        Block newBlock = new Block(newBlockId, 0, 
            firstBlock.getGenerationStamp());
        namenode.updateBlockForPipeline(newBlock, "");
        Assert.fail("Cannot get a new GS from a non-existent block");
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().contains("does not exist"));
      }

      
      /* Test RBW replicas */
      // change first block to a RBW
      DFSOutputStream out = null;
      try {
        out = (DFSOutputStream)(fileSys.append(file).
            getWrappedStream()); 
        out.write(1);
        out.hflush();
        FSDataInputStream in = null;
        try {
          in = fileSys.open(file);
          firstBlock = DFSTestUtil.getAllBlocks(in).get(0).getBlock();
        } finally {
          IOUtils.closeStream(in);
        }

        // test non-lease holder
        DFSClient dfs = ((DistributedFileSystem)fileSys).dfs;
        try {
          namenode.updateBlockForPipeline(firstBlock, "test" + dfs.clientName);
          Assert.fail("Cannot get a new GS for a non lease holder");
        } catch (LeaseExpiredException e) {
          Assert.assertTrue(e.getMessage().startsWith("Lease mismatch"));
        }

        // test null lease holder
        try {
          namenode.updateBlockForPipeline(firstBlock, null);
          Assert.fail("Cannot get a new GS for a null lease holder");
        } catch (LeaseExpiredException e) {
          Assert.assertTrue(e.getMessage().startsWith("Lease mismatch"));
        }

        // test getNewStampAndToken on a rbw block
        namenode.updateBlockForPipeline(firstBlock, dfs.clientName);
      } finally {
        IOUtils.closeStream(out);
      }
    } finally {
      cluster.shutdown();
    }
  }
}
