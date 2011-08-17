/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Enum representing queue state
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum QueueState {

  STOPPED("stopped"), RUNNING("running"), UNDEFINED("undefined");
  private final String stateName;
  private static Map<String, QueueState> enumMap =
      new HashMap<String, QueueState>();

  static {
    for (QueueState state : QueueState.values()) {
      enumMap.put(state.getStateName(), state);
    }
  }

  QueueState(String stateName) {
    this.stateName = stateName;
  }

  /**
   * @return the stateName
   */
  public String getStateName() {
    return stateName;
  }

  public static QueueState getState(String state) {
    QueueState qState = enumMap.get(state);
    if (qState == null) {
      return UNDEFINED;
    }
    return qState;
  }

  @Override
  public String toString() {
    return stateName;
  }

}