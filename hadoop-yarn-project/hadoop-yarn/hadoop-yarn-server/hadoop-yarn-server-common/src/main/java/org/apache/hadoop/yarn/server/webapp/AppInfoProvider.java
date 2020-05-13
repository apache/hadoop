/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.servlet.http.HttpServletRequest;

/**
 * Classes implementing this interface are used in the {@link LogServlet}
 * for providing various application related information.
 */
@InterfaceAudience.LimitedPrivate({"YARN"})
@InterfaceStability.Unstable
public interface AppInfoProvider {

  /**
   * Returns the node HTTP address.
   *
   * @param req {@link HttpServletRequest} associated with the request
   * @param appId the id of the application
   * @param appAttemptId the id of the application attempt
   * @param containerId the container id
   * @param clusterId the id of the cluster
   * @return the node HTTP address
   */
  String getNodeHttpAddress(HttpServletRequest req,
      String appId, String appAttemptId, String containerId, String clusterId);

  /**
   * Returns {@link BasicAppInfo} object that wraps the collected information
   * about the application.
   *
   * @param req {@link HttpServletRequest} associated with the request
   * @param appId the id of the application
   * @param clusterId the id of the cluster
   * @return {@link BasicAppInfo} object
   */
  BasicAppInfo getApp(HttpServletRequest req, String appId, String clusterId);
}
