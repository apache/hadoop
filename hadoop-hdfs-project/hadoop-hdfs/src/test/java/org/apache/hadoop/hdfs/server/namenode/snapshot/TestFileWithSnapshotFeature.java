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

import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.test.Whitebox;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static org.apache.hadoop.fs.StorageType.DISK;
import static org.apache.hadoop.fs.StorageType.SSD;
import static org.mockito.Mockito.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFileWithSnapshotFeature {
  private static final int BLOCK_SIZE = 1024;
  private static final short REPL_3 = 3;
  private static final short REPL_1 = 1;

  @Test
  public void testUpdateQuotaAndCollectBlocks() {
    FileDiffList diffs = new FileDiffList();
    FileWithSnapshotFeature sf = new FileWithSnapshotFeature(diffs);
    FileDiff diff = mock(FileDiff.class);
    BlockStoragePolicySuite bsps = mock(BlockStoragePolicySuite.class);
    BlockStoragePolicy bsp = mock(BlockStoragePolicy.class);
    BlockInfo[] blocks = new BlockInfo[] {
        new BlockInfoContiguous(new Block(1, BLOCK_SIZE, 1), REPL_1)
    };
    BlockManager bm = mock(BlockManager.class);

    // No snapshot
    INodeFile file = mock(INodeFile.class);
    when(file.getFileWithSnapshotFeature()).thenReturn(sf);
    when(file.getBlocks()).thenReturn(blocks);
    when(file.getStoragePolicyID()).thenReturn((byte) 1);
    Whitebox.setInternalState(file, "header", (long) REPL_1 << 48);
    when(file.getPreferredBlockReplication()).thenReturn(REPL_1);

    when(bsps.getPolicy(anyByte())).thenReturn(bsp);
    INode.BlocksMapUpdateInfo collectedBlocks = mock(
        INode.BlocksMapUpdateInfo.class);
    ArrayList<INode> removedINodes = new ArrayList<>();
    INode.ReclaimContext ctx = new INode.ReclaimContext(
        bsps, collectedBlocks, removedINodes, null);
    sf.updateQuotaAndCollectBlocks(ctx, file, diff);
    QuotaCounts counts = ctx.quotaDelta().getCountsCopy();
    Assert.assertEquals(0, counts.getStorageSpace());
    Assert.assertTrue(counts.getTypeSpaces().allLessOrEqual(0));

    // INode only exists in the snapshot
    INodeFile snapshotINode = mock(INodeFile.class);
    Whitebox.setInternalState(snapshotINode, "header", (long) REPL_3 << 48);
    Whitebox.setInternalState(diff, "snapshotINode", snapshotINode);
    when(diff.getSnapshotINode()).thenReturn(snapshotINode);

    when(bsp.chooseStorageTypes(REPL_1))
        .thenReturn(Lists.newArrayList(SSD));
    when(bsp.chooseStorageTypes(REPL_3))
        .thenReturn(Lists.newArrayList(DISK));
    blocks[0].setReplication(REPL_3);
    sf.updateQuotaAndCollectBlocks(ctx, file, diff);
    counts = ctx.quotaDelta().getCountsCopy();
    Assert.assertEquals((REPL_3 - REPL_1) * BLOCK_SIZE,
                        counts.getStorageSpace());
    Assert.assertEquals(BLOCK_SIZE, counts.getTypeSpaces().get(DISK));
    Assert.assertEquals(-BLOCK_SIZE, counts.getTypeSpaces().get(SSD));
  }

}
