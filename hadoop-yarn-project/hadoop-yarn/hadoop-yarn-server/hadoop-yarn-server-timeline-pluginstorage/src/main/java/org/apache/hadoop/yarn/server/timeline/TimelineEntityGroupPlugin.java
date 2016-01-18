/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;

import java.util.Collection;
import java.util.Set;
import java.util.SortedSet;

/**
 * Plugin to map a requested query ( or an Entity/set of Entities ) to a CacheID.
 * The Cache ID is an identifier to the data set that needs to be queried to
 * serve the response for the query.
 */
public abstract class TimelineEntityGroupPlugin {

  /**
   * Get the {@link TimelineEntityGroupId}s for the data sets that need to be
   * scanned to serve the query.
   *
   * @param entityType Entity Type being queried
   * @param primaryFilter Primary filter being applied
   * @param secondaryFilters Secondary filters being applied in the query
   * @return {@link org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId}
   */
  public abstract Set<TimelineEntityGroupId> getTimelineEntityGroupId(
      String entityType, NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilters);

  /**
   * Get the {@link TimelineEntityGroupId}s for the data sets that need to be
   * scanned to serve the query.
   *
   * @param entityType Entity Type being queried
   * @param entityId Entity Id being requested
   * @return {@link org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId}
   */
  public abstract Set<TimelineEntityGroupId> getTimelineEntityGroupId(
      String entityId,
      String entityType);


  /**
   * Get the {@link TimelineEntityGroupId}s for the data sets that need to be
   * scanned to serve the query.
   *
   * @param entityType Entity Type being queried
   * @param entityIds Entity Ids being requested
   * @param eventTypes Event Types being requested
   * @return {@link org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId}
   */
  public abstract Set<TimelineEntityGroupId> getTimelineEntityGroupId(
      String entityType, SortedSet<String> entityIds,
      Set<String> eventTypes);


}
