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
package org.apache.hadoop.hdfs.server.common.sps;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to track the completion of block movement future tasks.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockStorageMovementTracker implements Runnable {
  private static final Logger LOG = LoggerFactory
      .getLogger(BlockStorageMovementTracker.class);
  private final CompletionService<BlockMovementAttemptFinished>
      moverCompletionService;
  private final BlocksMovementsStatusHandler blksMovementsStatusHandler;

  private volatile boolean running = true;

  /**
   * BlockStorageMovementTracker constructor.
   *
   * @param moverCompletionService
   *          completion service.
   * @param handler
   *          blocks movements status handler
   */
  public BlockStorageMovementTracker(
      CompletionService<BlockMovementAttemptFinished> moverCompletionService,
      BlocksMovementsStatusHandler handler) {
    this.moverCompletionService = moverCompletionService;
    this.blksMovementsStatusHandler = handler;
  }

  @Override
  public void run() {
    while (running) {
      try {
        Future<BlockMovementAttemptFinished> future = moverCompletionService
            .take();
        if (future != null) {
          BlockMovementAttemptFinished result = future.get();
          LOG.debug("Completed block movement. {}", result);
          if (running && blksMovementsStatusHandler != null) {
            // handle completed block movement.
            blksMovementsStatusHandler.handle(result);
          }
        }
      } catch (InterruptedException e) {
        if (running) {
          LOG.error("Exception while moving block replica to target storage"
              + " type", e);
        }
      } catch (ExecutionException e) {
        // TODO: Do we need failure retries and implement the same if required.
        LOG.error("Exception while moving block replica to target storage type",
            e);
      }
    }
  }

  /**
   * Sets running flag to false.
   */
  public void stopTracking() {
    running = false;
  }
}
