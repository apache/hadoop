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

package org.apache.hadoop.yarn.client.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.impl.TimelineReaderClientImpl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A client library that can be used to get Timeline Entities associated with
 * application, application attempt or containers. This client library needs to
 * be used along with time line v.2 server version.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TimelineReaderClient extends CompositeService {

  /**
   * Create a new instance of Timeline Reader Client.
   */
  @InterfaceAudience.Public
  public static TimelineReaderClient createTimelineReaderClient() {
    return new TimelineReaderClientImpl();
  }

  @InterfaceAudience.Private
  public TimelineReaderClient(String name) {
    super(name);
  }

  /**
   * Gets application entity.
   * @param appId application id
   * @param fields Fields to be fetched. Defaults to INFO.
   * @param filters Filters to be applied while fetching entities.
   * @return entity of the application
   * @throws IOException
   */
  public abstract  TimelineEntity getApplicationEntity(
      ApplicationId appId, String fields, Map<String, String> filters)
      throws IOException;

  /**
   * Gets application attempt entity.
   * @param appAttemptId application attempt id
   * @param fields Fields to be fetched. Defaults to INFO.
   * @param filters Filters to be applied while fetching entities.
   * @return entity associated with application attempt
   * @throws IOException
   */
  public abstract  TimelineEntity getApplicationAttemptEntity(
      ApplicationAttemptId appAttemptId, String fields,
      Map<String, String> filters) throws IOException;

  /**
   * Gets application attempt entities.
   * @param appId application id
   * @param fields Fields to be fetched. Defaults to INFO.
   * @param filters Filters to be applied while fetching entities.
   * @param limit Number of entities to return.
   * @param fromId Retrieve next set of generic ids from given fromId
   * @return list of application attempt entities
   * @throws IOException
   */
  public abstract  List<TimelineEntity> getApplicationAttemptEntities(
      ApplicationId appId, String fields, Map<String, String> filters,
      long limit, String fromId) throws IOException;

  /**
   * Gets Timeline entity for the container.
   * @param containerId container id
   * @param fields Fields to be fetched. Defaults to INFO.
   * @param filters Filters to be applied while fetching entities.
   * @return timeline entity for container
   * @throws IOException
   */
  public abstract  TimelineEntity getContainerEntity(
      ContainerId containerId, String fields, Map<String, String> filters)
      throws IOException;

  /**
   * Gets container entities for an application.
   * @param appId application id
   * @param fields Fields to be fetched. Defaults to INFO.
   * @param filters Filters to be applied while fetching entities.
   * @param limit Number of entities to return.
   * @param fromId Retrieve next set of generic ids from given fromId
   * @return list of entities
   * @throws IOException
   */
  public abstract List<TimelineEntity> getContainerEntities(
      ApplicationId appId, String fields,
      Map<String, String> filters,
      long limit, String fromId) throws IOException;
}
