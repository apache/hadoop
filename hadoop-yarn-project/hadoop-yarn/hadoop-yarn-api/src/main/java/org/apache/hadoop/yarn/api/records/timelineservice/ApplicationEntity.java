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
 * This entity represents an application.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationEntity extends HierarchicalTimelineEntity {
  public static final String QUEUE_INFO_KEY =
      TimelineEntity.SYSTEM_INFO_KEY_PREFIX + "QUEUE";

  public ApplicationEntity() {
    super(TimelineEntityType.YARN_APPLICATION.toString());
  }

  public ApplicationEntity(TimelineEntity entity) {
    super(entity);
    if (!entity.getType().equals(
        TimelineEntityType.YARN_APPLICATION.toString())) {
      throw new IllegalArgumentException("Incompatible entity type: "
          + getId());
    }
  }

  public String getQueue() {
    return getInfo().get(QUEUE_INFO_KEY).toString();
  }

  public void setQueue(String queue) {
    addInfo(QUEUE_INFO_KEY, queue);
  }

  /**
   * Checks if the input TimelineEntity object is an ApplicationEntity.
   *
   * @param te TimelineEntity object.
   * @return true if input is an ApplicationEntity, false otherwise
   */
  public static boolean isApplicationEntity(TimelineEntity te) {
    return (te == null ? false
        : te.getType().equals(TimelineEntityType.YARN_APPLICATION.toString()));
  }

  /**
   * @param te TimelineEntity object.
   * @param eventId event with this id needs to be fetched
   * @return TimelineEvent if TimelineEntity contains the desired event.
   */
  public static TimelineEvent getApplicationEvent(TimelineEntity te,
      String eventId) {
    if (isApplicationEntity(te)) {
      for (TimelineEvent event : te.getEvents()) {
        if (event.getId().equals(eventId)) {
          return event;
        }
      }
    }
    return null;
  }
}
