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

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class MonitoredTaskImpl implements MonitoredTask {
  private long startTime;
  private long statusTime;
  private long stateTime;
  
  private volatile String status;
  private volatile String description;
  
  protected volatile State state = State.RUNNING;
  
  public MonitoredTaskImpl() {
    startTime = System.currentTimeMillis();
    statusTime = startTime;
    stateTime = startTime;
  }

  @Override
  public synchronized MonitoredTaskImpl clone() {
    try {
      return (MonitoredTaskImpl) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new AssertionError(); // Won't happen
    }
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
  public long getStatusTime() {
    return statusTime;
  }
  
  @Override
  public State getState() {
    return state;
  }
  
  @Override
  public long getStateTime() {
    return stateTime;
  }
  
  @Override
  public long getCompletionTimestamp() {
    if (state == State.COMPLETE || state == State.ABORTED) {
      return stateTime;
    }
    return -1;
  }

  @Override
  public void markComplete(String status) {
    setState(State.COMPLETE);
    setStatus(status);
  }

  @Override
  public void pause(String msg) {
    setState(State.WAITING);
    setStatus(msg);
  }

  @Override
  public void resume(String msg) {
    setState(State.RUNNING);
    setStatus(msg);
  }

  @Override
  public void abort(String msg) {
    setStatus(msg);
    setState(State.ABORTED);
  }
  
  @Override
  public void setStatus(String status) {
    this.status = status;
    statusTime = System.currentTimeMillis();
  }

  protected void setState(State state) {
    this.state = state;
    stateTime = System.currentTimeMillis();
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void cleanup() {
    if (state == State.RUNNING) {
      setState(State.ABORTED);
    }
  }

  /**
   * Force the completion timestamp backwards so that
   * it expires now.
   */
  public void expireNow() {
    stateTime -= 180 * 1000;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("description", getDescription());
    map.put("status", getStatus());
    map.put("state", getState());
    map.put("starttimems", getStartTime());
    map.put("statustimems", getCompletionTimestamp());
    map.put("statetimems", getCompletionTimestamp());
    return map;
  }

  @Override
  public String toJSON() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(toMap());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(512);
    sb.append(getDescription());
    sb.append(": status=");
    sb.append(getStatus());
    sb.append(", state=");
    sb.append(getState());
    sb.append(", startTime=");
    sb.append(getStartTime());
    sb.append(", completionTime=");
    sb.append(getCompletionTimestamp());
    return sb.toString();
  }

}
