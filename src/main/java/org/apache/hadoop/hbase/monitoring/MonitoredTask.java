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

import java.io.IOException;
import java.util.Map;

public interface MonitoredTask extends Cloneable {
  enum State {
    RUNNING,
    WAITING,
    COMPLETE,
    ABORTED;
  }

  public abstract long getStartTime();
  public abstract String getDescription();
  public abstract String getStatus();
  public abstract long getStatusTime();
  public abstract State getState();
  public abstract long getStateTime();
  public abstract long getCompletionTimestamp();

  public abstract void markComplete(String msg);
  public abstract void pause(String msg);
  public abstract void resume(String msg);
  public abstract void abort(String msg);
  public abstract void expireNow();

  public abstract void setStatus(String status);
  public abstract void setDescription(String description);

  /**
   * Explicitly mark this status as able to be cleaned up,
   * even though it might not be complete.
   */
  public abstract void cleanup();

  /**
   * Public exposure of Object.clone() in order to allow clients to easily 
   * capture current state.
   * @returns a copy of the object whose references will not change
   */
  public abstract MonitoredTask clone();

  /**
   * Creates a string map of internal details for extensible exposure of 
   * monitored tasks.
   * @return A Map containing information for this task.
   */
  public abstract Map<String, Object> toMap() throws IOException;

  /**
   * Creates a JSON object for parseable exposure of monitored tasks.
   * @return An encoded JSON object containing information for this task.
   */
  public abstract String toJSON() throws IOException;

}
