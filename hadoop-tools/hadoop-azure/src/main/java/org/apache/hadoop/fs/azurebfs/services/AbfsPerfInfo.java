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

import java.time.Instant;

import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;

/**
 * {@code AbfsPerfInfo} holds information on ADLS Gen 2 API performance observed by {@code AbfsClient}. Every
 * Abfs request keeps adding its information (success/failure, latency etc) to its {@code AbfsPerfInfo}'s object
 * as and when it becomes available. When the request is over, the performance information is recorded while
 * the {@code AutoCloseable} {@code AbfsPerfInfo} object is "closed".
 */
public final class AbfsPerfInfo implements AutoCloseable {

  // the tracker which will be extracting perf info out of this object.
  private AbfsPerfTracker abfsPerfTracker;

  // the caller name.
  private String callerName;

  // the callee name.
  private String calleeName;

  // time when this tracking started.
  private Instant trackingStart;

  // time when this tracking ended.
  private Instant trackingEnd;

  // whether the tracked request was successful.
  private boolean success;

  // time when the aggregate operation (to which this request belongs) started.
  private Instant aggregateStart;

  // number of requests in the aggregate operation (to which this request belongs).
  private long aggregateCount;

  // result of the request.
  private AbfsPerfLoggable res;

  public AbfsPerfInfo(AbfsPerfTracker abfsPerfTracker, String callerName, String calleeName) {
    this.callerName = callerName;
    this.calleeName = calleeName;
    this.abfsPerfTracker = abfsPerfTracker;
    this.success = false;
    this.trackingStart = abfsPerfTracker.getLatencyInstant();
  }

  public AbfsPerfInfo registerSuccess(boolean success) {
    this.success = success;
    return this;
  }

  public AbfsPerfInfo registerResult(AbfsPerfLoggable res) {
    this.res = res;
    return this;
  }

  public AbfsPerfInfo registerAggregates(Instant aggregateStart, long aggregateCount) {
    this.aggregateStart = aggregateStart;
    this.aggregateCount = aggregateCount;
    return this;
  }

  public AbfsPerfInfo finishTracking() {
    if (this.trackingEnd == null) {
      this.trackingEnd = abfsPerfTracker.getLatencyInstant();
    }

    return this;
  }

  public AbfsPerfInfo registerCallee(String calleeName) {
    this.calleeName = calleeName;
    return this;
  }

  @Override
  public void close() {
    abfsPerfTracker.trackInfo(this.finishTracking());
  }

  public String getCallerName() {
    return callerName;
  };

  public String getCalleeName() {
    return calleeName;
  }

  public Instant getTrackingStart() {
    return trackingStart;
  }

  public Instant getTrackingEnd() {
    return trackingEnd;
  }

  public boolean getSuccess() {
    return success;
  }

  public Instant getAggregateStart() {
    return aggregateStart;
  }

  public long getAggregateCount() {
    return aggregateCount;
  }

  public AbfsPerfLoggable getResult() {
    return res;
  }
}