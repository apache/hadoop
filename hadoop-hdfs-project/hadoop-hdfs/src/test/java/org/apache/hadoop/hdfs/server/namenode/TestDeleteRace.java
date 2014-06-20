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

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;


/**
 * Test race between delete and other operations.  For now only addBlock()
 * is tested since all others are acquiring FSNamesystem lock for the 
 * whole duration.
 */
public class TestDeleteRace {
  private static final Log LOG = LogFactory.getLog(TestDeleteRace.class);
  private static final Configuration conf = new HdfsConfiguration();
  private MiniDFSCluster cluster;

  @Test  
  public void testDeleteAddBlockRace() throws Exception {
    testDeleteAddBlockRace(false);
  }

  @Test  
  public void testDeleteAddBlockRaceWithSnapshot() throws Exception {
    testDeleteAddBlockRace(true);
  }

  private void testDeleteAddBlockRace(boolean hasSnapshot) throws Exception {
    try {
      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          SlowBlockPlacementPolicy.class, BlockPlacementPolicy.class);
      cluster = new MiniDFSCluster.Builder(conf).build();
      FileSystem fs = cluster.getFileSystem();
      final String fileName = "/testDeleteAddBlockRace";
      Path filePath = new Path(fileName);

      FSDataOutputStream out = null;
      out = fs.create(filePath);
      if (hasSnapshot) {
        SnapshotTestHelper.createSnapshot((DistributedFileSystem) fs, new Path(
            "/"), "s1");
      }

      Thread deleteThread = new DeleteThread(fs, filePath);
      deleteThread.start();

      try {
        // write data and syn to make sure a block is allocated.
        out.write(new byte[32], 0, 32);
        out.hsync();
        Assert.fail("Should have failed.");
      } catch (Exception e) {
        GenericTestUtils.assertExceptionContains(filePath.getName(), e);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static class SlowBlockPlacementPolicy extends
      BlockPlacementPolicyDefault {
    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                      int numOfReplicas,
                                      Node writer,
                                      List<DatanodeStorageInfo> chosenNodes,
                                      boolean returnChosenNodes,
                                      Set<Node> excludedNodes,
                                      long blocksize,
                                      StorageType storageType) {
      DatanodeStorageInfo[] results = super.chooseTarget(srcPath,
          numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
          blocksize, storageType);
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {}
      return results;
    }
  }

  private class DeleteThread extends Thread {
    private FileSystem fs;
    private Path path;

    DeleteThread(FileSystem fs, Path path) {
      this.fs = fs;
      this.path = path;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(1000);
        LOG.info("Deleting" + path);
        final FSDirectory fsdir = cluster.getNamesystem().dir;
        INode fileINode = fsdir.getINode4Write(path.toString());
        INodeMap inodeMap = (INodeMap) Whitebox.getInternalState(fsdir,
            "inodeMap");

        fs.delete(path, false);
        // after deletion, add the inode back to the inodeMap
        inodeMap.put(fileINode);
        LOG.info("Deleted" + path);
      } catch (Exception e) {
        LOG.info(e);
      }
    }
  }
}
