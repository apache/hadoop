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

import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMovementResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that block storage movement attempt failures are reported from DN and
 * processed them correctly or not.
 */
public class TestBlockStorageMovementAttemptedItems {

  private BlockStorageMovementAttemptedItems bsmAttemptedItems = null;
  private BlockStorageMovementNeeded unsatisfiedStorageMovementFiles = null;

  @Before
  public void setup() {
    unsatisfiedStorageMovementFiles = new BlockStorageMovementNeeded();
    bsmAttemptedItems = new BlockStorageMovementAttemptedItems(100, 500,
        unsatisfiedStorageMovementFiles);
    bsmAttemptedItems.start();
  }

  @After
  public void teardown() {
    if (bsmAttemptedItems != null) {
      bsmAttemptedItems.stop();
    }
  }

  private boolean checkItemMovedForRetry(Long item, long retryTimeout)
      throws InterruptedException {
    long stopTime = monotonicNow() + (retryTimeout * 2);
    boolean isItemFound = false;
    while (monotonicNow() < (stopTime)) {
      Long ele = null;
      while ((ele = unsatisfiedStorageMovementFiles.get()) != null) {
        if (item.longValue() == ele.longValue()) {
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
    Long item = new Long(1234);
    bsmAttemptedItems.add(item);
    bsmAttemptedItems.addResults(
        new BlocksStorageMovementResult[]{new BlocksStorageMovementResult(
            item.longValue(), BlocksStorageMovementResult.Status.FAILURE)});
    assertTrue(checkItemMovedForRetry(item, 200));
  }

  @Test(timeout = 30000)
  public void testAddResultWithSucessResult() throws Exception {
    Long item = new Long(1234);
    bsmAttemptedItems.add(item);
    bsmAttemptedItems.addResults(
        new BlocksStorageMovementResult[]{new BlocksStorageMovementResult(
            item.longValue(), BlocksStorageMovementResult.Status.SUCCESS)});
    assertFalse(checkItemMovedForRetry(item, 200));
  }

  @Test(timeout = 30000)
  public void testNoResultAdded() throws Exception {
    Long item = new Long(1234);
    bsmAttemptedItems.add(item);
    // After selfretry timeout, it should be added back for retry
    assertTrue(checkItemMovedForRetry(item, 600));
  }

}
