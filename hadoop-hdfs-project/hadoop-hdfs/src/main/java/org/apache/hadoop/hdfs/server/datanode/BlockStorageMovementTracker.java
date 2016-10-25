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
package org.apache.hadoop.hdfs.server.datanode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.StoragePolicySatisfyWorker.BlocksMovementsCompletionHandler;
import org.apache.hadoop.hdfs.server.datanode.StoragePolicySatisfyWorker.BlockMovementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to track the completion of block movement future tasks.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BlockStorageMovementTracker implements Runnable {
  private static final Logger LOG = LoggerFactory
      .getLogger(BlockStorageMovementTracker.class);
  private final CompletionService<BlockMovementResult> moverCompletionService;
  private final BlocksMovementsCompletionHandler blksMovementscompletionHandler;

  // Keeps the information - trackID vs its list of blocks
  private final Map<Long, List<Future<BlockMovementResult>>> moverTaskFutures;
  private final Map<Long, List<BlockMovementResult>> movementResults;

  /**
   * BlockStorageMovementTracker constructor.
   *
   * @param moverCompletionService
   *          completion service.
   * @param handler
   *          blocks movements completion handler
   */
  public BlockStorageMovementTracker(
      CompletionService<BlockMovementResult> moverCompletionService,
      BlocksMovementsCompletionHandler handler) {
    this.moverCompletionService = moverCompletionService;
    this.moverTaskFutures = new HashMap<>();
    this.blksMovementscompletionHandler = handler;
    this.movementResults = new HashMap<>();
  }

  @Override
  public void run() {
    while (true) {
      if (moverTaskFutures.size() <= 0) {
        try {
          synchronized (moverTaskFutures) {
            // Waiting for mover tasks.
            moverTaskFutures.wait(2000);
          }
        } catch (InterruptedException ignore) {
          // ignore
        }
      }
      try {
        Future<BlockMovementResult> future = moverCompletionService.take();
        if (future != null) {
          BlockMovementResult result = future.get();
          LOG.debug("Completed block movement. {}", result);
          long trackId = result.getTrackId();
          List<Future<BlockMovementResult>> blocksMoving = moverTaskFutures
              .get(trackId);
          blocksMoving.remove(future);

          List<BlockMovementResult> resultPerTrackIdList =
              addMovementResultToTrackIdList(result);

          // Completed all the scheduled blocks movement under this 'trackId'.
          if (blocksMoving.isEmpty()) {
            synchronized (moverTaskFutures) {
              moverTaskFutures.remove(trackId);
            }
            // handle completed blocks movements per trackId.
            blksMovementscompletionHandler.handle(resultPerTrackIdList);
            movementResults.remove(trackId);
          }
        }
      } catch (ExecutionException | InterruptedException e) {
        // TODO: Do we need failure retries and implement the same if required.
        LOG.error("Exception while moving block replica to target storage type",
            e);
      }
    }
  }

  private List<BlockMovementResult> addMovementResultToTrackIdList(
      BlockMovementResult result) {
    long trackId = result.getTrackId();
    List<BlockMovementResult> perTrackIdList = movementResults.get(trackId);
    if (perTrackIdList == null) {
      perTrackIdList = new ArrayList<>();
      movementResults.put(trackId, perTrackIdList);
    }
    perTrackIdList.add(result);
    return perTrackIdList;
  }

  /**
   * Add future task to the tracking list to check the completion status of the
   * block movement.
   *
   * @param trackID
   *          tracking Id
   * @param futureTask
   *          future task used for moving the respective block
   */
  void addBlock(long trackID, Future<BlockMovementResult> futureTask) {
    synchronized (moverTaskFutures) {
      List<Future<BlockMovementResult>> futures = moverTaskFutures
          .get(Long.valueOf(trackID));
      // null for the first task
      if (futures == null) {
        futures = new ArrayList<>();
        moverTaskFutures.put(trackID, futures);
      }
      futures.add(futureTask);
      // Notify waiting tracker thread about the newly added tasks.
      moverTaskFutures.notify();
    }
  }
}
