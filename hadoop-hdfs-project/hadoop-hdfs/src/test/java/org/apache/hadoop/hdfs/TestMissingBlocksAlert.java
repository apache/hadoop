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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.AvailableSpaceBlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.junit.Assert;
import org.junit.Test;

import javax.management.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The test makes sure that NameNode detects presense blocks that do not have
 * any valid replicas. In addition, it verifies that HDFS front page displays
 * a warning in such a case.
 */
public class TestMissingBlocksAlert {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMissingBlocksAlert.class);
  
  @Test
  public void testMissingBlocksAlert()
          throws IOException, InterruptedException,
                 MalformedObjectNameException, AttributeNotFoundException,
                 MBeanException, ReflectionException,
                 InstanceNotFoundException {
    
    MiniDFSCluster cluster = null;
    
    try {
      Configuration conf = new HdfsConfiguration();
      //minimize test delay
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
          0);
      conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
      int fileLen = 10*1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, fileLen/2);

      //start a cluster with single datanode
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      final BlockManager bm = cluster.getNamesystem().getBlockManager();
      DistributedFileSystem dfs =
          cluster.getFileSystem();

      // create a normal file
      DFSTestUtil.createFile(dfs, new Path("/testMissingBlocksAlert/file1"), 
                             fileLen, (short)3, 0);

      Path corruptFile = new Path("/testMissingBlocks/corruptFile");
      DFSTestUtil.createFile(dfs, corruptFile, fileLen, (short)3, 0);

      // Corrupt the block
      ExtendedBlock block = DFSTestUtil.getFirstBlock(dfs, corruptFile);
      cluster.corruptReplica(0, block);

      // read the file so that the corrupt block is reported to NN
      FSDataInputStream in = dfs.open(corruptFile); 
      try {
        in.readFully(new byte[fileLen]);
      } catch (ChecksumException ignored) { // checksum error is expected.      
      }
      in.close();

      LOG.info("Waiting for missing blocks count to increase...");

      while (dfs.getMissingBlocksCount() <= 0) {
        Thread.sleep(100);
      }
      assertTrue(dfs.getMissingBlocksCount() == 1);
      assertEquals(4, dfs.getLowRedundancyBlocksCount());
      assertEquals(3, bm.getUnderReplicatedNotMissingBlocks());

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
              "Hadoop:service=NameNode,name=NameNodeInfo");
      Assert.assertEquals(1, (long)(Long) mbs.getAttribute(mxbeanName,
                      "NumberOfMissingBlocks"));

      // now do the reverse : remove the file expect the number of missing 
      // blocks to go to zero

      dfs.delete(corruptFile, true);

      LOG.info("Waiting for missing blocks count to be zero...");
      while (dfs.getMissingBlocksCount() > 0) {
        Thread.sleep(100);
      }

      assertEquals(2, dfs.getLowRedundancyBlocksCount());
      assertEquals(2, bm.getUnderReplicatedNotMissingBlocks());

      Assert.assertEquals(0, (long)(Long) mbs.getAttribute(mxbeanName,
              "NumberOfMissingBlocks"));

      Path replOneFile = new Path("/testMissingBlocks/replOneFile");
      DFSTestUtil.createFile(dfs, replOneFile, fileLen, (short)1, 0);
      ExtendedBlock replOneBlock = DFSTestUtil.getFirstBlock(
          dfs, replOneFile);
      cluster.corruptReplica(0, replOneBlock);

      // read the file so that the corrupt block is reported to NN
      in = dfs.open(replOneFile);
      try {
        in.readFully(new byte[fileLen]);
      } catch (ChecksumException ignored) { // checksum error is expected.
      }
      in.close();
      assertEquals(1, dfs.getMissingReplOneBlocksCount());
      Assert.assertEquals(1, (long)(Long) mbs.getAttribute(mxbeanName,
          "NumberOfMissingBlocksWithReplicationFactorOne"));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testMissReplicatedBlockwithTwoRack() throws Exception {
    Configuration conf = new Configuration();
    //Start cluster with rack /default/rack1
    String[] hosts = new String[] {"host0", "host1", "host2", "host3"};
    String[] racks = new String[] {"/default/rack1", "/default/rack1",
        "/default/rack1", "/default/rack1"};
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        AvailableSpaceBlockPlacementPolicy.class.getName());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4)
        .hosts(hosts).racks(racks).build();
    Path file = new Path("/file2");
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      DFSTestUtil.createFile(dfs, file, 1024, (short) 2, 0);
      dfs.getFileStatus(file);
      //Add one more rack /default/rack2
      cluster.startDataNodes(conf, 2, true, null,
          new String[] {"/default/rack2", "/default/rack2"},
          new String[] {"host4", "host5"}, null);
      dfs.setReplication(file, (short) 3);
      // wait for block replication
      DFSTestUtil.waitForReplication(dfs, file, (short) 3, 60000);
    } finally {
      cluster.shutdown();
    }
  }
}
