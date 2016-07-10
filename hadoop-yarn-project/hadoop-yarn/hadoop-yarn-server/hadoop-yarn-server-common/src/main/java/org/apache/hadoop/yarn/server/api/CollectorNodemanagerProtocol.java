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
package org.apache.hadoop.yarn.server.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoResponse;

/**
 * <p>The protocol between an <code>TimelineCollectorManager</code> and a
 * <code>NodeManager</code> to report a new application collector get launched.
 * </p>
 *
 */
@Private
public interface CollectorNodemanagerProtocol {

  /**
   *
   * <p>
   * The <code>TimelineCollectorManager</code> provides a list of mapping
   * between application and collector's address in
   * {@link ReportNewCollectorInfoRequest} to a <code>NodeManager</code> to
   * <em>register</em> collector's info, include: applicationId and REST URI to
   * access collector. NodeManager will add them into registered collectors
   * and register them into <code>ResourceManager</code> afterwards.
   * </p>
   *
   * @param request the request of registering a new collector or a list of
   *                collectors
   * @return the response for registering the new collector
   * @throws YarnException if the request is invalid
   * @throws IOException if there are I/O errors
   */
  ReportNewCollectorInfoResponse reportNewCollectorInfo(
      ReportNewCollectorInfoRequest request)
      throws YarnException, IOException;

  /**
   * <p>
   * The collector needs to get the context information including user, flow
   * and flow run ID to associate with every incoming put-entity requests.
   * </p>
   * @param request the request of getting the aggregator context information of
   *                the given application
   * @return the response for registering the new collector
   * @throws YarnException if the request is invalid
   * @throws IOException if there are I/O errors
   */
  GetTimelineCollectorContextResponse getTimelineCollectorContext(
      GetTimelineCollectorContextRequest request)
      throws YarnException, IOException;
}
