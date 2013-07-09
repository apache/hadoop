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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request from clients to get a report of Applications
 * in the cluster from the <code>ResourceManager</code>.</p>
 *
 *
 * @see ApplicationClientProtocol#getApplications(GetApplicationsRequest)
 */
@Public
@Stable
public abstract class GetApplicationsRequest {
  @Public
  @Stable
  public static GetApplicationsRequest newInstance() {
    GetApplicationsRequest request =
        Records.newRecord(GetApplicationsRequest.class);
    return request;
  }

  @Public
  @Stable
  public static GetApplicationsRequest newInstance(
      Set<String> applicationTypes) {
    GetApplicationsRequest request =
        Records.newRecord(GetApplicationsRequest.class);
    request.setApplicationTypes(applicationTypes);
    return request;
  }

  /**
   * Get the application types to filter applications on
   *
   * @return Set of Application Types to filter on
   */
  @Public
  @Stable
  public abstract Set<String> getApplicationTypes();

  /**
   * Set the application types to filter applications on
   *
   * @param applicationTypes
   * A Set of Application Types to filter on.
   * If not defined, match all applications
   */
  @Private
  @Unstable
  public abstract void
      setApplicationTypes(Set<String> applicationTypes);
}
