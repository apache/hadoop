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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.impl.TimelineV2ClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * A client library that can be used to post some information in terms of a
 * number of conceptual entities. This client library needs to be used along
 * with time line v.2 server version.
 * Refer {@link TimelineClient} for ATS V1 interface.
 */
public abstract class TimelineV2Client extends CompositeService {
  /**
   * Creates an instance of the timeline v.2 client.
   *
   * @param appId the application id with which the timeline client is
   *          associated
   * @return the created timeline client instance
   */
  @Public
  public static TimelineV2Client createTimelineClient(ApplicationId appId) {
    TimelineV2Client client = new TimelineV2ClientImpl(appId);
    return client;
  }

  protected TimelineV2Client(String name) {
    super(name);
  }

  /**
   * <p>
   * Send the information of a number of conceptual entities within the scope
   * of YARN application to the timeline service v.2 collector. It is a blocking
   * API. The method will not return until all the put entities have been
   * persisted.
   * </p>
   *
   * @param entities the collection of {@link TimelineEntity}
   * @throws IOException  if there are I/O errors
   * @throws YarnException if entities are incomplete/invalid
   */
  @Public
  public abstract void putEntities(TimelineEntity... entities)
      throws IOException, YarnException;

  /**
   * <p>
   * Send the information of a number of conceptual entities within the scope
   * of YARN application to the timeline service v.2 collector. It is an
   * asynchronous API. The method will return once all the entities are
   * received.
   * </p>
   *
   * @param entities the collection of {@link TimelineEntity}
   * @throws IOException  if there are I/O errors
   * @throws YarnException if entities are incomplete/invalid
   */
  @Public
  public abstract void putEntitiesAsync(TimelineEntity... entities)
      throws IOException, YarnException;

  /**
   * <p>
   * Update collector info received in AllocateResponse which contains the
   * timeline service address where the request will be sent to and the timeline
   * delegation token which will be used to send the request.
   * </p>
   *
   * @param collectorInfo Collector info which contains the timeline service
   * address and timeline delegation token.
   */
  public abstract void setTimelineCollectorInfo(CollectorInfo collectorInfo);


  /**
   * <p>
   * Send the information of a number of conceptual entities within the scope of
   * a sub-application to the timeline service v.2 collector. It is a blocking
   * API. The method will not return until all the put entities have been
   * persisted.
   * </p>
   *
   * @param entities the collection of {@link TimelineEntity}
   * @throws IOException  if there are I/O errors
   * @throws YarnException if entities are incomplete/invalid
   */
  @Public
  public abstract void putSubAppEntities(TimelineEntity... entities)
      throws IOException, YarnException;

  /**
   * <p>
   * Send the information of a number of conceptual entities within the scope of
   * a sub-application to the timeline service v.2 collector. It is an
   * asynchronous API. The method will return once all the entities are received
   * .
   * </p>
   *
   * @param entities the collection of {@link TimelineEntity}
   * @throws IOException  if there are I/O errors
   * @throws YarnException if entities are incomplete/invalid
   */
  @Public
  public abstract void putSubAppEntitiesAsync(TimelineEntity... entities)
      throws IOException, YarnException;
}
