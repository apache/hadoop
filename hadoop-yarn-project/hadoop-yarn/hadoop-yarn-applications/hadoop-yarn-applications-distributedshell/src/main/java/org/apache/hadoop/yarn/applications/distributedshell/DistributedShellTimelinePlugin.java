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

package org.apache.hadoop.yarn.applications.distributedshell;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineEntityGroupPlugin;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

/**
 * Timeline v1.5 reader plugin for YARN distributed shell. It tranlsates an
 * incoming getEntity request to a set of related timeline entity groups, via
 * the information provided in the primary filter or entity id field.
 */
public class DistributedShellTimelinePlugin extends TimelineEntityGroupPlugin {

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters) {
    if (ApplicationMaster.DSEntity.DS_CONTAINER.toString().equals(entityType)) {
      if (primaryFilter == null) {
        return null;
      }
      return toEntityGroupId(primaryFilter.getValue().toString());
    }
    return null;
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityId,
      String entityType) {
    if (ApplicationMaster.DSEntity.DS_CONTAINER.toString().equals(entityId)) {
      ContainerId containerId = ContainerId.fromString(entityId);
      ApplicationId appId = containerId.getApplicationAttemptId()
          .getApplicationId();
      return toEntityGroupId(appId.toString());
    }
    return null;
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityType,
      SortedSet<String> entityIds, Set<String> eventTypes) {
    // Right now this method is not used by TimelineEntityGroupPlugin
    return null;
  }

  private Set<TimelineEntityGroupId> toEntityGroupId(String strAppId) {
    ApplicationId appId = ApplicationId.fromString(strAppId);
    TimelineEntityGroupId groupId = TimelineEntityGroupId.newInstance(
        appId, ApplicationMaster.CONTAINER_ENTITY_GROUP_ID);
    Set<TimelineEntityGroupId> result = new HashSet<>();
    result.add(groupId);
    return result;
  }
}
