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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.util.AutoCloseableLock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for VolumeReplicaMap.
 */
public class TestVolumeReplicaMap {

  private VolumeReplicaMap map;
  private String bpid = "BPID-0";
  private FsVolumeImpl mockVol = mock(FsVolumeImpl.class);

  @Before
  public void initialize() {
    map = new VolumeReplicaMap(new AutoCloseableLock());
  }

  @Test
  public void testAddGet() {
    try {
      map.add(bpid, null);
      fail("Expected exception not thrown");
    } catch (IllegalArgumentException expected) { }

    long baseBlockId = 100, genStamp = 1001, length = 100;
    int numBlocks = 10;
    for (int num = 0; num < numBlocks; num++) {
      ReplicaInfo replica = new ReplicaBuilder(
          HdfsServerConstants.ReplicaState.FINALIZED)
          .setBlock(new Block(baseBlockId + num, length, genStamp))
          .setFsVolume(mockVol)
          .build();
      map.add(bpid, replica);
    }

    for (int num = 0; num < numBlocks; num++) {
      assertNotNull(map.get(bpid, baseBlockId + num));
    }

    // test for a block with an existing block id but different gen stamp.
    assertNull(map.get(bpid, new Block(baseBlockId, length, 0)));
  }

  @Test
  public void testRemove() {
    final Block block = new Block(1234, 1234, 1234);
    map.add(bpid, new FinalizedReplica(block, mockVol, null));
    // Test 1: null argument throws invalid argument exception
    try {
      map.remove(bpid, null);
      fail("Expected exception not thrown");
    } catch (IllegalArgumentException expected) { }

    // Test 2: remove failure - generation stamp mismatch
    Block b = new Block(block);
    b.setGenerationStamp(0);
    assertNull(map.remove(bpid, b));

    // Test 3: remove failure - blockID mismatch
    b.setGenerationStamp(block.getGenerationStamp());
    b.setBlockId(0);
    assertNull(map.remove(bpid, b));

    // Test 4: remove success
    assertNotNull(map.remove(bpid, block));

    // Test 5: remove failure - invalid blockID
    assertNull(map.remove(bpid, 0));

    // Test 6: remove success
    map.add(bpid, new FinalizedReplica(block, mockVol, null));
    assertNotNull(map.remove(bpid, block.getBlockId()));
  }
}
