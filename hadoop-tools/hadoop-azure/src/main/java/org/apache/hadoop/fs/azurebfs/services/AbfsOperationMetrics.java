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

  private AtomicLong bytesFailed;

  private AtomicLong bytesSuccessful;

  private AtomicLong operationsFailed;

  private AtomicLong operationsSuccessful;

  private long endTime;

  private long startTime;

  AbfsOperationMetrics(long startTime) {
    this.startTime = startTime;
    this.bytesFailed = new AtomicLong();
    this.bytesSuccessful = new AtomicLong();
    this.operationsFailed = new AtomicLong();
    this.operationsSuccessful = new AtomicLong();
  }

  AtomicLong getBytesFailed() {
    return bytesFailed;
  }

  AtomicLong getBytesSuccessful() {
    return bytesSuccessful;
  }

  AtomicLong getOperationsFailed() {
    return operationsFailed;
  }

  AtomicLong getOperationsSuccessful() {
    return operationsSuccessful;
  }

  long getEndTime() {
    return endTime;
  }

  void setEndTime(final long endTime) {
    this.endTime = endTime;
  }

  long getStartTime() {
    return startTime;
  }
}
