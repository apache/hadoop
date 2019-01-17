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
package org.apache.hadoop.hdfs.server.namenode.sps;

import static org.apache.hadoop.util.Time.monotonicNow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.StorageTypeNodePair;
import org.apache.hadoop.hdfs.server.sps.ExternalSPSContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests that block storage movement attempt failures are reported from DN and
 * processed them correctly or not.
 */
public class TestBlockStorageMovementAttemptedItems {

  private BlockStorageMovementAttemptedItems bsmAttemptedItems;
  private BlockStorageMovementNeeded unsatisfiedStorageMovementFiles;
  private final int selfRetryTimeout = 500;

  @Before
  public void setup() throws Exception {
    Configuration config = new HdfsConfiguration();
    Context ctxt = Mockito.mock(ExternalSPSContext.class);
    SPSService sps = new StoragePolicySatisfier(config);
    Mockito.when(ctxt.isRunning()).thenReturn(true);
    Mockito.when(ctxt.isInSafeMode()).thenReturn(false);
    Mockito.when(ctxt.isFileExist(Mockito.anyLong())).thenReturn(true);
    unsatisfiedStorageMovementFiles =
        new BlockStorageMovementNeeded(ctxt);
    bsmAttemptedItems = new BlockStorageMovementAttemptedItems(sps,
        unsatisfiedStorageMovementFiles, ctxt);
  }

  @After
  public void teardown() {
    if (bsmAttemptedItems != null) {
      bsmAttemptedItems.stop();
      bsmAttemptedItems.stopGracefully();
    }
  }

  private boolean checkItemMovedForRetry(Long item, long retryTimeout)
      throws InterruptedException {
    long stopTime = monotonicNow() + (retryTimeout * 2);
    boolean isItemFound = false;
    while (monotonicNow() < (stopTime)) {
      ItemInfo ele = null;
      while ((ele = unsatisfiedStorageMovementFiles.get()) != null) {
        if (item == ele.getFile()) {
          isItemFound = true;
          break;
        }
      }
      if (!isItemFound) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
    return isItemFound;
  }

  /**
   * Verify that moved blocks reporting should queued up the block info.
   */
  @Test(timeout = 30000)
  public void testAddReportedMoveAttemptFinishedBlocks() throws Exception {
    Long item = new Long(1234);
    Block block = new Block(item);
    DatanodeInfo dnInfo = DFSTestUtil.getLocalDatanodeInfo(9867);
    Set<StorageTypeNodePair> locs = new HashSet<>();
    locs.add(new StorageTypeNodePair(StorageType.ARCHIVE, dnInfo));
    Map<Block, Set<StorageTypeNodePair>> blocksMap = new HashMap<>();
    blocksMap.put(block, locs);
    bsmAttemptedItems.add(0L, 0L, 0L, blocksMap, 0);
    bsmAttemptedItems.notifyReportedBlock(dnInfo, StorageType.ARCHIVE,
        block);
    assertEquals("Failed to receive result!", 1,
        bsmAttemptedItems.getMovementFinishedBlocksCount());
  }

  /**
   * Verify empty moved blocks reporting queue.
   */
  @Test(timeout = 30000)
  public void testNoBlockMovementAttemptFinishedReportAdded() throws Exception {
    Long item = new Long(1234);
    Block block = new Block(item);
    DatanodeInfo dnInfo = DFSTestUtil.getLocalDatanodeInfo(9867);
    Set<StorageTypeNodePair> locs = new HashSet<>();
    locs.add(new StorageTypeNodePair(StorageType.ARCHIVE, dnInfo));
    Map<Block, Set<StorageTypeNodePair>> blocksMap = new HashMap<>();
    blocksMap.put(block, locs);
    bsmAttemptedItems.add(0L, 0L, 0L, blocksMap, 0);
    assertEquals("Shouldn't receive result", 0,
        bsmAttemptedItems.getMovementFinishedBlocksCount());
    assertEquals("Item doesn't exist in the attempted list", 1,
        bsmAttemptedItems.getAttemptedItemsCount());
  }

  /**
   * Partial block movement with
   * BlockMovementStatus#DN_BLK_STORAGE_MOVEMENT_SUCCESS. Here, first occurrence
   * is #blockStorageMovementReportedItemsCheck() and then
   * #blocksStorageMovementUnReportedItemsCheck().
   */
  @Test(timeout = 30000)
  public void testPartialBlockMovementShouldBeRetried1() throws Exception {
    Long item = new Long(1234);
    Block block1 = new Block(item);
    Block block2 = new Block(5678L);
    Long trackID = 0L;
    DatanodeInfo dnInfo = DFSTestUtil.getLocalDatanodeInfo(9867);
    Set<StorageTypeNodePair> locs = new HashSet<>();
    locs.add(new StorageTypeNodePair(StorageType.ARCHIVE, dnInfo));
    Map<Block, Set<StorageTypeNodePair>> blocksMap = new HashMap<>();
    blocksMap.put(block1, locs);
    blocksMap.put(block2, locs);
    bsmAttemptedItems.add(trackID, trackID, 0L, blocksMap, 0);
    bsmAttemptedItems.notifyReportedBlock(dnInfo, StorageType.ARCHIVE,
        block1);

    // start block movement report monitor thread
    bsmAttemptedItems.start();
    assertTrue("Failed to add to the retry list",
        checkItemMovedForRetry(trackID, 5000));
    assertEquals("Failed to remove from the attempted list", 0,
        bsmAttemptedItems.getAttemptedItemsCount());
  }

  /**
   * Partial block movement. Here, first occurrence is
   * #blocksStorageMovementUnReportedItemsCheck() and then
   * #blockStorageMovementReportedItemsCheck().
   */
  @Test(timeout = 30000)
  public void testPartialBlockMovementShouldBeRetried2() throws Exception {
    Long item = new Long(1234);
    Block block = new Block(item);
    Long trackID = 0L;
    DatanodeInfo dnInfo = DFSTestUtil.getLocalDatanodeInfo(9867);
    Set<StorageTypeNodePair> locs = new HashSet<>();
    locs.add(new StorageTypeNodePair(StorageType.ARCHIVE, dnInfo));
    Map<Block, Set<StorageTypeNodePair>> blocksMap = new HashMap<>();
    blocksMap.put(block, locs);
    bsmAttemptedItems.add(trackID, trackID, 0L, blocksMap, 0);
    bsmAttemptedItems.notifyReportedBlock(dnInfo, StorageType.ARCHIVE,
        block);

    Thread.sleep(selfRetryTimeout * 2); // Waiting to get timed out

    bsmAttemptedItems.blocksStorageMovementUnReportedItemsCheck();
    bsmAttemptedItems.blockStorageMovementReportedItemsCheck();

    assertTrue("Failed to add to the retry list",
        checkItemMovedForRetry(trackID, 5000));
    assertEquals("Failed to remove from the attempted list", 0,
        bsmAttemptedItems.getAttemptedItemsCount());
  }

  /**
   * Partial block movement with only BlocksStorageMoveAttemptFinished report
   * and storageMovementAttemptedItems list is empty.
   */
  @Test(timeout = 30000)
  public void testPartialBlockMovementWithEmptyAttemptedQueue()
      throws Exception {
    Long item = new Long(1234);
    Block block = new Block(item);
    Long trackID = 0L;
    DatanodeInfo dnInfo = DFSTestUtil.getLocalDatanodeInfo(9867);
    Set<StorageTypeNodePair> locs = new HashSet<>();
    locs.add(new StorageTypeNodePair(StorageType.ARCHIVE, dnInfo));
    Map<Block, Set<StorageTypeNodePair>> blocksMap = new HashMap<>();
    blocksMap.put(block, locs);
    bsmAttemptedItems.add(trackID, trackID, 0L, blocksMap, 0);
    bsmAttemptedItems.notifyReportedBlock(dnInfo, StorageType.ARCHIVE,
        block);
    assertFalse(
        "Should not add in queue again if it is not there in"
            + " storageMovementAttemptedItems",
        checkItemMovedForRetry(trackID, 5000));
    assertEquals("Failed to remove from the attempted list", 1,
        bsmAttemptedItems.getAttemptedItemsCount());
  }
}
