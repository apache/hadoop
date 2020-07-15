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

import org.apache.hadoop.ipc.metrics.RpcMetrics;

/**
 * Implement this interface to be used for RPC scheduling and backoff.
 *
 */
public interface RpcScheduler {
  /**
   * Returns priority level greater than zero as a hint for scheduling.
   */
  int getPriorityLevel(Schedulable obj);

  boolean shouldBackOff(Schedulable obj);

  /**
   * This method only exists to maintain backwards compatibility with old
   * implementations. It will not be called by any Hadoop code, and should not
   * be implemented by new implementations.
   *
   * @deprecated Use
   * {@link #addResponseTime(String, Schedulable, ProcessingDetails)} instead.
   */
  @Deprecated
  @SuppressWarnings("unused")
  default void addResponseTime(String name, int priorityLevel, int queueTime,
      int processingTime) {
    throw new UnsupportedOperationException(
        "This method is deprecated: use the other addResponseTime");
  }

  /**
   * Store a processing time value for an RPC call into this scheduler.
   *
   * @param callName The name of the call.
   * @param schedulable The schedulable representing the incoming call.
   * @param details The details of processing time.
   */
  @SuppressWarnings("deprecation")
  default void addResponseTime(String callName, Schedulable schedulable,
      ProcessingDetails details) {
    // For the sake of backwards compatibility with old implementations of
    // this interface, a default implementation is supplied which uses the old
    // method. All new implementations MUST override this interface and should
    // NOT use the other addResponseTime method.
    int queueTime = (int)
        details.get(ProcessingDetails.Timing.QUEUE, RpcMetrics.TIMEUNIT);
    int processingTime = (int)
        details.get(ProcessingDetails.Timing.PROCESSING, RpcMetrics.TIMEUNIT);
    addResponseTime(callName, schedulable.getPriorityLevel(),
        queueTime, processingTime);
  }

  void stop();
}
