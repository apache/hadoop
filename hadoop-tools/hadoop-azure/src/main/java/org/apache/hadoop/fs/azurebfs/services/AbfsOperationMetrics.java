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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Stores Abfs operation metrics during each analysis period.
 */
class AbfsOperationMetrics {

  /**
   * No of bytes which could not be transferred due to a failed operation.
  */
  private final AtomicLong bytesFailed;

  /**
   * No of bytes successfully transferred during a successful operation.
   */
  private final AtomicLong bytesSuccessful;

  /**
   * Total no of failed operations.
   */
  private final AtomicLong operationsFailed;

  /**
   * Total no of successful operations.
   */
  private final AtomicLong operationsSuccessful;

  /**
   * Time when collection of metrics ended.
   */
  private long endTime;

  /**
   * Time when the collection of metrics started.
   */
  private final long startTime;

  AbfsOperationMetrics(long startTime) {
    this.startTime = startTime;
    this.bytesFailed = new AtomicLong();
    this.bytesSuccessful = new AtomicLong();
    this.operationsFailed = new AtomicLong();
    this.operationsSuccessful = new AtomicLong();
  }

  /**
   *
   * @return bytes failed to transfer.
   */
  AtomicLong getBytesFailed() {
    return bytesFailed;
  }

  /**
   *
   * @return bytes successfully transferred.
   */
  AtomicLong getBytesSuccessful() {
    return bytesSuccessful;
  }

  /**
   *
   * @return no of operations failed.
   */
  AtomicLong getOperationsFailed() {
    return operationsFailed;
  }

  /**
   *
   * @return no of successful operations.
   */
  AtomicLong getOperationsSuccessful() {
    return operationsSuccessful;
  }

  /**
   *
   * @return end time of metric collection.
   */
  long getEndTime() {
    return endTime;
  }

  /**
   *
   * @param endTime sets the end time.
   */
  void setEndTime(final long endTime) {
    this.endTime = endTime;
  }

  /**
   *
   * @return start time of metric collection.
   */
  long getStartTime() {
    return startTime;
  }

  void addBytesFailed(long bytes) {
    this.getBytesFailed().addAndGet(bytes);
  }

  void addBytesSuccessful(long bytes) {
    this.getBytesSuccessful().addAndGet(bytes);
  }

  void incrementOperationsFailed() {
    this.getOperationsFailed().incrementAndGet();
  }

  void incrementOperationsSuccessful() {
    this.getOperationsSuccessful().incrementAndGet();
  }

}

