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

import static org.apache.hadoop.util.Time.monotonicNow;
import static org.junit.Assert.*;

import org.apache.hadoop.hdfs.server.namenode.StoragePolicySatisfier.ItemInfo;
import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMovementResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests that block storage movement attempt failures are reported from DN and
 * processed them correctly or not.
 */
public class TestBlockStorageMovementAttemptedItems {

  private BlockStorageMovementAttemptedItems bsmAttemptedItems = null;
  private BlockStorageMovementNeeded unsatisfiedStorageMovementFiles = null;
  private final int selfRetryTimeout = 500;

  @Before
  public void setup() throws Exception {
    unsatisfiedStorageMovementFiles = new BlockStorageMovementNeeded(
        Mockito.mock(Namesystem.class),
        Mockito.mock(StoragePolicySatisfier.class), 100);
    StoragePolicySatisfier sps = Mockito.mock(StoragePolicySatisfier.class);
    bsmAttemptedItems = new BlockStorageMovementAttemptedItems(100,
        selfRetryTimeout, unsatisfiedStorageMovementFiles, sps);
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
        if (item == ele.getTrackId()) {
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

  @Test(timeout = 30000)
  public void testAddResultWithFailureResult() throws Exception {
    bsmAttemptedItems.start(); // start block movement result monitor thread
    Long item = new Long(1234);
    bsmAttemptedItems.add(new ItemInfo(0L, item), true);
    bsmAttemptedItems.addResults(
        new BlocksStorageMovementResult[]{new BlocksStorageMovementResult(
            item.longValue(), BlocksStorageMovementResult.Status.FAILURE)});
    assertTrue(checkItemMovedForRetry(item, 200));
  }

  @Test(timeout = 30000)
  public void testAddResultWithSucessResult() throws Exception {
    bsmAttemptedItems.start(); // start block movement result monitor thread
    Long item = new Long(1234);
    bsmAttemptedItems.add(new ItemInfo(0L, item), true);
    bsmAttemptedItems.addResults(
        new BlocksStorageMovementResult[]{new BlocksStorageMovementResult(
            item.longValue(), BlocksStorageMovementResult.Status.SUCCESS)});
    assertFalse(checkItemMovedForRetry(item, 200));
  }

  @Test(timeout = 30000)
  public void testNoResultAdded() throws Exception {
    bsmAttemptedItems.start(); // start block movement result monitor thread
    Long item = new Long(1234);
    bsmAttemptedItems.add(new ItemInfo(0L, item), true);
    // After self retry timeout, it should be added back for retry
    assertTrue("Failed to add to the retry list",
        checkItemMovedForRetry(item, 600));
    assertEquals("Failed to remove from the attempted list", 0,
        bsmAttemptedItems.getAttemptedItemsCount());
  }

  /**
   * Partial block movement with BlocksStorageMovementResult#SUCCESS. Here,
   * first occurrence is #blockStorageMovementResultCheck() and then
   * #blocksStorageMovementUnReportedItemsCheck().
   */
  @Test(timeout = 30000)
  public void testPartialBlockMovementShouldBeRetried1() throws Exception {
    Long item = new Long(1234);
    bsmAttemptedItems.add(new ItemInfo(0L, item), false);
    bsmAttemptedItems.addResults(
        new BlocksStorageMovementResult[]{new BlocksStorageMovementResult(
            item.longValue(), BlocksStorageMovementResult.Status.SUCCESS)});

    // start block movement result monitor thread
    bsmAttemptedItems.start();
    assertTrue("Failed to add to the retry list",
        checkItemMovedForRetry(item, 5000));
    assertEquals("Failed to remove from the attempted list", 0,
        bsmAttemptedItems.getAttemptedItemsCount());
  }

  /**
   * Partial block movement with BlocksStorageMovementResult#SUCCESS. Here,
   * first occurrence is #blocksStorageMovementUnReportedItemsCheck() and then
   * #blockStorageMovementResultCheck().
   */
  @Test(timeout = 30000)
  public void testPartialBlockMovementShouldBeRetried2() throws Exception {
    Long item = new Long(1234);
    bsmAttemptedItems.add(new ItemInfo(0L, item), false);
    bsmAttemptedItems.addResults(
        new BlocksStorageMovementResult[]{new BlocksStorageMovementResult(
            item.longValue(), BlocksStorageMovementResult.Status.SUCCESS)});

    Thread.sleep(selfRetryTimeout * 2); // Waiting to get timed out

    bsmAttemptedItems.blocksStorageMovementUnReportedItemsCheck();
    bsmAttemptedItems.blockStorageMovementResultCheck();

    assertTrue("Failed to add to the retry list",
        checkItemMovedForRetry(item, 5000));
    assertEquals("Failed to remove from the attempted list", 0,
        bsmAttemptedItems.getAttemptedItemsCount());
  }

  /**
   * Partial block movement with only BlocksStorageMovementResult#FAILURE
   * result and storageMovementAttemptedItems list is empty.
   */
  @Test(timeout = 30000)
  public void testPartialBlockMovementWithEmptyAttemptedQueue()
      throws Exception {
    Long item = new Long(1234);
    bsmAttemptedItems.addResults(
        new BlocksStorageMovementResult[]{new BlocksStorageMovementResult(
            item, BlocksStorageMovementResult.Status.FAILURE)});
    bsmAttemptedItems.blockStorageMovementResultCheck();
    assertFalse(
        "Should not add in queue again if it is not there in"
            + " storageMovementAttemptedItems",
        checkItemMovedForRetry(item, 5000));
    assertEquals("Failed to remove from the attempted list", 0,
        bsmAttemptedItems.getAttemptedItemsCount());
  }

  /**
   * Partial block movement with BlocksStorageMovementResult#FAILURE result and
   * storageMovementAttemptedItems.
   */
  @Test(timeout = 30000)
  public void testPartialBlockMovementShouldBeRetried4() throws Exception {
    Long item = new Long(1234);
    bsmAttemptedItems.add(new ItemInfo(0L, item), false);
    bsmAttemptedItems.addResults(
        new BlocksStorageMovementResult[]{new BlocksStorageMovementResult(
            item.longValue(), BlocksStorageMovementResult.Status.FAILURE)});
    bsmAttemptedItems.blockStorageMovementResultCheck();
    assertTrue("Failed to add to the retry list",
        checkItemMovedForRetry(item, 5000));
    assertEquals("Failed to remove from the attempted list", 0,
        bsmAttemptedItems.getAttemptedItemsCount());
  }
}
