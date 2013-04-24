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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;

/**
 * Test if FSNamesystem handles heartbeat right
 */
public class TestComputeInvalidateWork {
  /**
   * Test if {@link FSNamesystem#computeInvalidateWork(int)}
   * can schedule invalidate work correctly 
   */
  @Test
  public void testCompInvalidate() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final int NUM_OF_DATANODES = 3;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES).build();
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final BlockManager bm = namesystem.getBlockManager();
      final int blockInvalidateLimit = bm.getDatanodeManager().blockInvalidateLimit;
      final DatanodeDescriptor[] nodes = bm.getDatanodeManager(
          ).getHeartbeatManager().getDatanodes();
      assertEquals(nodes.length, NUM_OF_DATANODES);
      
      namesystem.writeLock();
      try {
        for (int i=0; i<nodes.length; i++) {
          for(int j=0; j<3*blockInvalidateLimit+1; j++) {
            Block block = new Block(i*(blockInvalidateLimit+1)+j, 0, 
                GenerationStamp.LAST_RESERVED_STAMP);
            bm.addToInvalidates(block, nodes[i]);
          }
        }
        
        assertEquals(blockInvalidateLimit*NUM_OF_DATANODES, 
            bm.computeInvalidateWork(NUM_OF_DATANODES+1));
        assertEquals(blockInvalidateLimit*NUM_OF_DATANODES, 
            bm.computeInvalidateWork(NUM_OF_DATANODES));
        assertEquals(blockInvalidateLimit*(NUM_OF_DATANODES-1), 
            bm.computeInvalidateWork(NUM_OF_DATANODES-1));
        int workCount = bm.computeInvalidateWork(1);
        if (workCount == 1) {
          assertEquals(blockInvalidateLimit+1, bm.computeInvalidateWork(2));
        } else {
          assertEquals(workCount, blockInvalidateLimit);
          assertEquals(2, bm.computeInvalidateWork(2));
        }
      } finally {
        namesystem.writeUnlock();
      }
    } finally {
      cluster.shutdown();
    }
  }
}
