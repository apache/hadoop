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
 * This entity represents a user defined entities to be stored under sub
 * application table.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SubApplicationEntity extends HierarchicalTimelineEntity {

  public static final String YARN_APPLICATION_ID = "YARN_APPLICATION_ID";

  public SubApplicationEntity(TimelineEntity entity) {
    super(entity);
  }

  /**
   * Checks if the input TimelineEntity object is an SubApplicationEntity.
   *
   * @param te TimelineEntity object.
   * @return true if input is an SubApplicationEntity, false otherwise
   */
  public static boolean isSubApplicationEntity(TimelineEntity te) {
    return (te != null && te instanceof SubApplicationEntity);
  }

  public void setApplicationId(String appId) {
    addInfo(YARN_APPLICATION_ID, appId);
  }
}
