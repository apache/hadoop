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

/**
 * Class to hold extra output stream configs.
 */
public class AbfsOutputStreamContext extends AbfsStreamContext {

  private int writeBufferSize;

  private boolean enableFlush;

  private boolean enableSmallWriteOptimization;

  private boolean disableOutputStreamFlush;

  private AbfsOutputStreamStatistics streamStatistics;

  private boolean isAppendBlob;

  private int writeMaxConcurrentRequestCount;

  private int maxWriteRequestsToQueue;

  private AbfsLease lease;

  public AbfsOutputStreamContext(final long sasTokenRenewPeriodForStreamsInSeconds) {
    super(sasTokenRenewPeriodForStreamsInSeconds);
  }

  public AbfsOutputStreamContext withWriteBufferSize(
          final int writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    return this;
  }

  public AbfsOutputStreamContext enableFlush(final boolean enableFlush) {
    this.enableFlush = enableFlush;
    return this;
  }

  public AbfsOutputStreamContext enableSmallWriteOptimization(final boolean enableSmallWriteOptimization) {
    this.enableSmallWriteOptimization = enableSmallWriteOptimization;
    return this;
  }

  public AbfsOutputStreamContext disableOutputStreamFlush(
          final boolean disableOutputStreamFlush) {
    this.disableOutputStreamFlush = disableOutputStreamFlush;
    return this;
  }

  public AbfsOutputStreamContext withStreamStatistics(
      final AbfsOutputStreamStatistics streamStatistics) {
    this.streamStatistics = streamStatistics;
    return this;
  }

  public AbfsOutputStreamContext withAppendBlob(
          final boolean isAppendBlob) {
    this.isAppendBlob = isAppendBlob;
    return this;
  }

  public AbfsOutputStreamContext build() {
    // Validation of parameters to be done here.
    return this;
  }

  public AbfsOutputStreamContext withWriteMaxConcurrentRequestCount(
      final int writeMaxConcurrentRequestCount) {
    this.writeMaxConcurrentRequestCount = writeMaxConcurrentRequestCount;
    return this;
  }

  public AbfsOutputStreamContext withMaxWriteRequestsToQueue(
      final int maxWriteRequestsToQueue) {
    this.maxWriteRequestsToQueue = maxWriteRequestsToQueue;
    return this;
  }

  public AbfsOutputStreamContext withLease(final AbfsLease lease) {
    this.lease = lease;
    return this;
  }

  public int getWriteBufferSize() {
    return writeBufferSize;
  }

  public boolean isEnableFlush() {
    return enableFlush;
  }

  public boolean isDisableOutputStreamFlush() {
    return disableOutputStreamFlush;
  }

  public AbfsOutputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  public boolean isAppendBlob() {
    return isAppendBlob;
  }

  public int getWriteMaxConcurrentRequestCount() {
    return this.writeMaxConcurrentRequestCount;
  }

  public int getMaxWriteRequestsToQueue() {
    return this.maxWriteRequestsToQueue;
  }

  public boolean isEnableSmallWriteOptimization() {
    return this.enableSmallWriteOptimization;
  }

  public AbfsLease getLease() {
    return this.lease;
  }

  public String getLeaseId() {
    if (this.lease == null) {
      return null;
    }
    return this.lease.getLeaseID();
  }
}
