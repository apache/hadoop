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

package org.apache.hadoop.ipc;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines which queue to start reading from, occasionally drawing from
 * low-priority queues in order to prevent starvation. Given the pull pattern
 * [9, 4, 1] for 3 queues:
 *
 * The cycle is (a minimum of) 9+4+1=14 reads.
 * Queue 0 is read (at least) 9 times
 * Queue 1 is read (at least) 4 times
 * Queue 2 is read (at least) 1 time
 * Repeat
 *
 * There may be more reads than the minimum due to race conditions. This is
 * allowed by design for performance reasons.
 */
public class WeightedRoundRobinMultiplexer implements RpcMultiplexer {
  // Config keys
  public static final String IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY =
    "faircallqueue.multiplexer.weights";

  public static final Logger LOG =
      LoggerFactory.getLogger(WeightedRoundRobinMultiplexer.class);

  private final int numQueues; // The number of queues under our provisioning

  private final AtomicInteger currentQueueIndex; // Current queue we're serving
  private final AtomicInteger requestsLeft; // Number of requests left for this queue

  private int[] queueWeights; // The weights for each queue
  private int[] sharedQueueWeights; // The weights for each shared queue

  public WeightedRoundRobinMultiplexer(int aNumQueues, String ns,
      Configuration conf) {
    this(aNumQueues, 0, ns, conf);
  }

  public WeightedRoundRobinMultiplexer(int numSharedQueues, int numReservedQueues, String ns,
    Configuration conf) {
    if (numSharedQueues <= 0) {
      throw new IllegalArgumentException("Requested queues (" + numSharedQueues +
        ") must be greater than zero.");
    }
    if (numReservedQueues < 0) {
      throw new IllegalArgumentException("Requested reserved queues (" + numReservedQueues +
          ") must be equal or greater than zero.");
    }
    // The weights for each shared queue
    this.sharedQueueWeights = parseSharedQueueWeights(ns, conf, numSharedQueues);
    // The weights for each reserved queue
    int[] reservedQueueWeights =
        parseReservedQueueWeights(ns, conf, numReservedQueues);

    // Merge weights of shared and reserved queues into one
    this.numQueues = numSharedQueues + numReservedQueues;
    this.queueWeights = new int[numQueues];
    System.arraycopy(sharedQueueWeights, 0, this.queueWeights, 0, numSharedQueues);
    System.arraycopy(reservedQueueWeights, 0, this.queueWeights, numSharedQueues, numReservedQueues);

    this.currentQueueIndex = new AtomicInteger(0);
    this.requestsLeft = new AtomicInteger(this.queueWeights[0]);

    LOG.info("WeightedRoundRobinMultiplexer is being used.");
  }

  private int[] parseSharedQueueWeights(String ns, Configuration conf, int numSharedQueues) {
    int[] sharedQueueWeights = conf.getInts(ns + "." +
        IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY);
    if (sharedQueueWeights.length == 0) {
      sharedQueueWeights = getDefaultQueueWeights(numSharedQueues);
    } else if (sharedQueueWeights.length != numSharedQueues) {
      throw new IllegalArgumentException(ns + "." +
          IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY + " must specify exactly " +
          numSharedQueues + " weights: one for each priority level.");
    }
    return sharedQueueWeights;
  }

  private int[] parseReservedQueueWeights(String ns, Configuration conf, int numReservedQueues) {
    int[] reservedQueueWeights = conf.getInts(ns + "." + CommonConfigurationKeys.IPC_CALLQUEUE_WRRMUX_RESERVED_WEIGHTS_KEY);
    if (reservedQueueWeights.length == 0) {
      reservedQueueWeights = getDefaultQueueWeights(numReservedQueues);
    } else if (reservedQueueWeights.length != numReservedQueues) {
      throw new IllegalArgumentException(ns + "." +
          CommonConfigurationKeys.IPC_CALLQUEUE_WRRMUX_RESERVED_WEIGHTS_KEY + " must specify exactly " +
          numReservedQueues + " weight(s): one for each reserved user)");
    }
    return reservedQueueWeights;
  }

  /**
   * Creates default weights for each queue. The weights are 2^N.
   */
  private int[] getDefaultQueueWeights(int aNumQueues) {
    int[] weights = new int[aNumQueues];

    int weight = 1; // Start low
    for(int i = aNumQueues - 1; i >= 0; i--) { // Start at lowest queue
      weights[i] = weight;
      weight *= 2; // Double every iteration
    }
    return weights;
  }

  /**
   * Move to the next queue.
   */
  private void moveToNextQueue() {
    int thisIdx = this.currentQueueIndex.get();

    // Wrap to fit in our bounds
    int nextIdx = (thisIdx + 1) % this.numQueues;

    // Set to next index: once this is called, requests will start being
    // drawn from nextIdx, but requestsLeft will continue to decrement into
    // the negatives
    this.currentQueueIndex.set(nextIdx);

    // Finally, reset requestsLeft. This will enable moveToNextQueue to be
    // called again, for the new currentQueueIndex
    this.requestsLeft.set(this.queueWeights[nextIdx]);
    LOG.debug("Moving to next queue from queue index {} to index {}, " +
        "number of requests left for current queue: {}.",
        thisIdx, nextIdx, requestsLeft);
  }

  /**
   * Advances the index, which will change the current index
   * if called enough times.
   */
  private void advanceIndex() {
    // Since we did read, we should decrement
    int requestsLeftVal = this.requestsLeft.decrementAndGet();

    // Strict compare with zero (instead of inequality) so that if another
    // thread decrements requestsLeft, only one thread will be responsible
    // for advancing currentQueueIndex
    if (requestsLeftVal == 0) {
      // This is guaranteed to be called exactly once per currentQueueIndex
      this.moveToNextQueue();
    }
  }

  /**
   * Gets the current index. Should be accompanied by a call to
   * advanceIndex at some point.
   */
  private int getCurrentIndex() {
    return this.currentQueueIndex.get();
  }

  /**
   * Use the mux by getting and advancing index.
   */
  public int getAndAdvanceCurrentIndex() {
    int idx = this.getCurrentIndex();
    this.advanceIndex();
    return idx;
  }

}
