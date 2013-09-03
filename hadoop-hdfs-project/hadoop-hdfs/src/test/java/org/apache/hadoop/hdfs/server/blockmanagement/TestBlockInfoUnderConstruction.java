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

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.junit.Test;

/**
 * This class provides tests for BlockInfoUnderConstruction class
 */
public class TestBlockInfoUnderConstruction {
  @Test
  public void testInitializeBlockRecovery() throws Exception {
    DatanodeStorageInfo s1 = DFSTestUtil.createDatanodeStorageInfo("10.10.1.1", "s1");
    DatanodeDescriptor dd1 = s1.getDatanodeDescriptor();
    DatanodeStorageInfo s2 = DFSTestUtil.createDatanodeStorageInfo("10.10.1.2", "s2");
    DatanodeDescriptor dd2 = s2.getDatanodeDescriptor();
    DatanodeStorageInfo s3 = DFSTestUtil.createDatanodeStorageInfo("10.10.1.3", "s3");
    DatanodeDescriptor dd3 = s3.getDatanodeDescriptor();

    dd1.isAlive = dd2.isAlive = dd3.isAlive = true;
    BlockInfoUnderConstruction blockInfo = new BlockInfoUnderConstruction(
        new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP),
        3,
        BlockUCState.UNDER_CONSTRUCTION,
        new DatanodeStorageInfo[] {s1, s2, s3});

    // Recovery attempt #1.
    long currentTime = System.currentTimeMillis();
    dd1.setLastUpdate(currentTime - 3 * 1000);
    dd2.setLastUpdate(currentTime - 1 * 1000);
    dd3.setLastUpdate(currentTime - 2 * 1000);
    blockInfo.initializeBlockRecovery(1);
    BlockInfoUnderConstruction[] blockInfoRecovery = dd2.getLeaseRecoveryCommand(1);
    assertEquals(blockInfoRecovery[0], blockInfo);

    // Recovery attempt #2.
    currentTime = System.currentTimeMillis();
    dd1.setLastUpdate(currentTime - 2 * 1000);
    dd2.setLastUpdate(currentTime - 1 * 1000);
    dd3.setLastUpdate(currentTime - 3 * 1000);
    blockInfo.initializeBlockRecovery(2);
    blockInfoRecovery = dd1.getLeaseRecoveryCommand(1);
    assertEquals(blockInfoRecovery[0], blockInfo);

    // Recovery attempt #3.
    currentTime = System.currentTimeMillis();
    dd1.setLastUpdate(currentTime - 2 * 1000);
    dd2.setLastUpdate(currentTime - 1 * 1000);
    dd3.setLastUpdate(currentTime - 3 * 1000);
    currentTime = System.currentTimeMillis();
    blockInfo.initializeBlockRecovery(3);
    blockInfoRecovery = dd3.getLeaseRecoveryCommand(1);
    assertEquals(blockInfoRecovery[0], blockInfo);

    // Recovery attempt #4.
    // Reset everything. And again pick DN with most recent heart beat.
    currentTime = System.currentTimeMillis();
    dd1.setLastUpdate(currentTime - 2 * 1000);
    dd2.setLastUpdate(currentTime - 1 * 1000);
    dd3.setLastUpdate(currentTime);
    currentTime = System.currentTimeMillis();
    blockInfo.initializeBlockRecovery(3);
    blockInfoRecovery = dd3.getLeaseRecoveryCommand(1);
    assertEquals(blockInfoRecovery[0], blockInfo);
  }
}
