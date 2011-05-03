/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.monitoring;

import com.google.common.annotations.VisibleForTesting;

class MonitoredTaskImpl implements MonitoredTask {
  private long startTime;
  private long completionTimestamp = -1;
  
  private String status;
  private String description;
  
  private State state = State.RUNNING;
  
  public MonitoredTaskImpl() {
    startTime = System.currentTimeMillis();
  }

  @Override
  public long getStartTime() {
    return startTime;
  }
  
  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getStatus() {
    return status;
  }
  
  @Override
  public State getState() {
    return state;
  }
  
  @Override
  public long getCompletionTimestamp() {
    return completionTimestamp;
  }
  
  @Override
  public void markComplete(String status) {
    state = State.COMPLETE;
    setStatus(status);
    completionTimestamp = System.currentTimeMillis();
  }

  @Override
  public void abort(String msg) {
    setStatus(msg);
    state = State.ABORTED;
    completionTimestamp = System.currentTimeMillis();
  }
  
  @Override
  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void cleanup() {
    if (state == State.RUNNING) {
      state = State.ABORTED;
      completionTimestamp = System.currentTimeMillis();
    }
  }

  /**
   * Force the completion timestamp backwards so that
   * it expires now.
   */
  @VisibleForTesting
  void expireNow() {
    completionTimestamp -= 180 * 1000;
  }
}
