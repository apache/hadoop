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

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import junit.framework.TestCase;

/**
 * Test if FSNamesystem handles heartbeat right
 */
public class TestHeartbeatHandling extends TestCase {
  /**
   * Test if {@link FSNamesystem#handleHeartbeat(DatanodeRegistration, long, long, long, int, int)}
   * can pick up replication and/or invalidate requests and 
   * observes the max limit
   */
  public void testHeartbeat() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final DatanodeRegistration nodeReg = cluster.getDataNodes().get(0).dnRegistration;
      DatanodeDescriptor dd = namesystem.getDatanode(nodeReg);
      
      final int REMAINING_BLOCKS = 1;
      final int MAX_REPLICATE_LIMIT = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 2);
      final int MAX_INVALIDATE_LIMIT = FSNamesystem.BLOCK_INVALIDATE_CHUNK;
      final int MAX_INVALIDATE_BLOCKS = 2*MAX_INVALIDATE_LIMIT+REMAINING_BLOCKS;
      final int MAX_REPLICATE_BLOCKS = 2*MAX_REPLICATE_LIMIT+REMAINING_BLOCKS;
      final DatanodeDescriptor[] ONE_TARGET = new DatanodeDescriptor[1];

      synchronized (namesystem.heartbeats) {
      for (int i=0; i<MAX_REPLICATE_BLOCKS; i++) {
        dd.addBlockToBeReplicated(
            new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP), ONE_TARGET);
      }
      DatanodeCommand[] cmds = namesystem.handleHeartbeat(
          nodeReg, dd.getCapacity(), dd.getDfsUsed(), dd.getRemaining(), 0, 0);
      assertEquals(1, cmds.length);
      assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
      assertEquals(MAX_REPLICATE_LIMIT, ((BlockCommand)cmds[0]).getBlocks().length);
      
      ArrayList<Block> blockList = new ArrayList<Block>(MAX_INVALIDATE_BLOCKS);
      for (int i=0; i<MAX_INVALIDATE_BLOCKS; i++) {
        blockList.add(new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP));
      }
      dd.addBlocksToBeInvalidated(blockList);
           
      cmds = namesystem.handleHeartbeat(
          nodeReg, dd.getCapacity(), dd.getDfsUsed(), dd.getRemaining(), 0, 0);
      assertEquals(2, cmds.length);
      assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
      assertEquals(MAX_REPLICATE_LIMIT, ((BlockCommand)cmds[0]).getBlocks().length);
      assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[1].getAction());
      assertEquals(MAX_INVALIDATE_LIMIT, ((BlockCommand)cmds[1]).getBlocks().length);
      
      cmds = namesystem.handleHeartbeat(
          nodeReg, dd.getCapacity(), dd.getDfsUsed(), dd.getRemaining(), 0, 0);
      assertEquals(2, cmds.length);
      assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
      assertEquals(REMAINING_BLOCKS, ((BlockCommand)cmds[0]).getBlocks().length);
      assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[1].getAction());
      assertEquals(MAX_INVALIDATE_LIMIT, ((BlockCommand)cmds[1]).getBlocks().length);
      
      cmds = namesystem.handleHeartbeat(
          nodeReg, dd.getCapacity(), dd.getDfsUsed(), dd.getRemaining(), 0, 0);
      assertEquals(1, cmds.length);
      assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[0].getAction());
      assertEquals(REMAINING_BLOCKS, ((BlockCommand)cmds[0]).getBlocks().length);

      cmds = namesystem.handleHeartbeat(
          nodeReg, dd.getCapacity(), dd.getDfsUsed(), dd.getRemaining(), 0, 0);
      assertEquals(null, cmds);
      }
    } finally {
      cluster.shutdown();
    }
  }
}
