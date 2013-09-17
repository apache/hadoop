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

package org.apache.hadoop.mapred;

import java.util.List;

/**
 * Abstract QueueRefresher class. Scheduler's can extend this and return an
 * instance of this in the {@link #getQueueRefresher()} method. The
 * {@link #refreshQueues(List)} method of this instance will be invoked by the
 * {@link QueueManager} whenever it gets a request from an administrator to
 * refresh its own queue-configuration. This method has a documented contract
 * between the {@link QueueManager} and the {@link TaskScheduler}.
 * 
 * Before calling QueueRefresher, the caller must hold the lock to the
 * corresponding {@link TaskScheduler} (generally in the {@link JobTracker}).
 */
abstract class QueueRefresher {

  /**
   * Refresh the queue-configuration in the scheduler. This method has the
   * following contract.
   * <ol>
   * <li>Before this method, {@link QueueManager} does a validation of the new
   * queue-configuration. For e.g, currently addition of new queues, or
   * removal of queues at any level in the hierarchy is not supported by
   * {@link QueueManager} and so are not supported for schedulers too.</li>
   * <li>Schedulers will be passed a list of {@link JobQueueInfo}s of the root
   * queues i.e. the queues at the top level. All the descendants are properly
   * linked from these top-level queues.</li>
   * <li>Schedulers should use the scheduler specific queue properties from
   * the newRootQueues, validate the properties themselves and apply them
   * internally.</li>
   * <li>
   * Once the method returns successfully from the schedulers, it is assumed
   * that the refresh of queue properties is successful throughout and will be
   * 'committed' internally to {@link QueueManager} too. It is guaranteed that
   * at no point, after successful return from the scheduler, is the queue
   * refresh in QueueManager failed. If ever, such abnormalities happen, the
   * queue framework will be inconsistent and will need a JT restart.</li>
   * <li>If scheduler throws an exception during {@link #refreshQueues()},
   * {@link QueueManager} throws away the newly read configuration, retains
   * the old (consistent) configuration and informs the request issuer about
   * the error appropriately.</li>
   * </ol>
   * 
   * @param newRootQueues
   */
  abstract void refreshQueues(List<JobQueueInfo> newRootQueues)
      throws Throwable;
}