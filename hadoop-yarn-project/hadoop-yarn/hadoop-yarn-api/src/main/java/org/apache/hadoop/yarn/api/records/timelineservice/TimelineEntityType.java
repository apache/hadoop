/*
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
package org.apache.hadoop.yarn.api.records.timelineservice;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Defines type of entity.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum TimelineEntityType {
  YARN_CLUSTER,
  YARN_FLOW_RUN,
  YARN_APPLICATION,
  YARN_APPLICATION_ATTEMPT,
  YARN_CONTAINER,
  YARN_USER,
  YARN_QUEUE,
  YARN_FLOW_ACTIVITY;

  /**
   * Whether the input type can be a parent of this entity.
   *
   * @param type entity type.
   * @return true, if this entity type is parent of passed entity type, false
   *     otherwise.
   */
  public boolean isParent(TimelineEntityType type) {
    switch (this) {
    case YARN_CLUSTER:
      return false;
    case YARN_FLOW_RUN:
      return YARN_FLOW_RUN == type || YARN_CLUSTER == type;
    case YARN_APPLICATION:
      return YARN_FLOW_RUN == type || YARN_CLUSTER == type;
    case YARN_APPLICATION_ATTEMPT:
      return YARN_APPLICATION == type;
    case YARN_CONTAINER:
      return YARN_APPLICATION_ATTEMPT == type;
    case YARN_QUEUE:
      return YARN_QUEUE == type;
    default:
      return false;
    }
  }

  /**
   * Whether the input type can be a child of this entity.
   *
   * @param type entity type.
   * @return true, if this entity type is child of passed entity type, false
   *     otherwise.
   */
  public boolean isChild(TimelineEntityType type) {
    switch (this) {
    case YARN_CLUSTER:
      return YARN_FLOW_RUN == type || YARN_APPLICATION == type;
    case YARN_FLOW_RUN:
      return YARN_FLOW_RUN == type || YARN_APPLICATION == type;
    case YARN_APPLICATION:
      return YARN_APPLICATION_ATTEMPT == type;
    case YARN_APPLICATION_ATTEMPT:
      return YARN_CONTAINER == type;
    case YARN_CONTAINER:
      return false;
    case YARN_QUEUE:
      return YARN_QUEUE == type;
    default:
      return false;
    }
  }

  /**
   * Whether the type of this entity matches the type indicated by the input
   * argument.
   *
   * @param typeString entity type represented as a string.
   * @return true, if string representation of this entity type matches the
   *     entity type passed.
   */
  public boolean matches(String typeString) {
    return toString().equals(typeString);
  }
}
