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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.IOUtils;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import org.junit.Test;

public class TestUpdatePipelineWithSnapshots {
  
  // Regression test for HDFS-6647.
  @Test
  public void testUpdatePipelineAfterDelete() throws Exception {
    Configuration conf = new HdfsConfiguration();
    Path file = new Path("/test-file");    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    
    try {
      FileSystem fs = cluster.getFileSystem();
      NamenodeProtocols namenode = cluster.getNameNodeRpc();
      DFSOutputStream out = null;
      try {
        // Create a file and make sure a block is allocated for it.
        out = (DFSOutputStream)(fs.create(file).
            getWrappedStream()); 
        out.write(1);
        out.hflush();
        
        // Create a snapshot that includes the file.
        SnapshotTestHelper.createSnapshot((DistributedFileSystem) fs,
            new Path("/"), "s1");
        
        // Grab the block info of this file for later use.
        FSDataInputStream in = null;
        ExtendedBlock oldBlock = null;
        try {
          in = fs.open(file);
          oldBlock = DFSTestUtil.getAllBlocks(in).get(0).getBlock();
        } finally {
          IOUtils.closeStream(in);
        }
        
        // Allocate a new block ID/gen stamp so we can simulate pipeline
        // recovery.
        String clientName = ((DistributedFileSystem)fs).getClient()
            .getClientName();
        LocatedBlock newLocatedBlock = namenode.updateBlockForPipeline(
            oldBlock, clientName);
        ExtendedBlock newBlock = new ExtendedBlock(oldBlock.getBlockPoolId(),
            oldBlock.getBlockId(), oldBlock.getNumBytes(), 
            newLocatedBlock.getBlock().getGenerationStamp());

        // Delete the file from the present FS. It will still exist the
        // previously-created snapshot. This will log an OP_DELETE for the
        // file in question.
        fs.delete(file, true);
        
        // Simulate a pipeline recovery, wherein a new block is allocated
        // for the existing block, resulting in an OP_UPDATE_BLOCKS being
        // logged for the file in question.
        try {
          namenode.updatePipeline(clientName, oldBlock, newBlock,
              newLocatedBlock.getLocations(), newLocatedBlock.getStorageIDs());
        } catch (IOException ioe) {
          // normal
          assertExceptionContains(
              "does not exist or it is not under construction", ioe);
        }
        
        // Make sure the NN can restart with the edit logs as we have them now.
        cluster.restartNameNode(true);
      } finally {
        IOUtils.closeStream(out);
      }
    } finally {
      cluster.shutdown();
    }
  }

}
