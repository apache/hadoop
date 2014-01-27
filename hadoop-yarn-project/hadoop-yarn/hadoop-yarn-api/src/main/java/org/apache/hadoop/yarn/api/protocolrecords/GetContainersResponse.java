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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a client requesting
 * a list of {@link ContainerReport} for containers.
 * </p>
 * 
 * <p>
 * The <code>ContainerReport</code> for each container includes the container
 * details.
 * </p>
 * 
 * @see ContainerReport
 * @see ApplicationHistoryProtocol#getContainers(GetContainersRequest)
 */
@Public
@Unstable
public abstract class GetContainersResponse {

  @Public
  @Unstable
  public static GetContainersResponse newInstance(
      List<ContainerReport> containers) {
    GetContainersResponse response =
        Records.newRecord(GetContainersResponse.class);
    response.setContainerList(containers);
    return response;
  }

  /**
   * Get a list of <code>ContainerReport</code> for all the containers of an
   * application attempt.
   * 
   * @return a list of <code>ContainerReport</code> for all the containers of an
   *         application attempt
   * 
   */
  @Public
  @Unstable
  public abstract List<ContainerReport> getContainerList();

  /**
   * Set a list of <code>ContainerReport</code> for all the containers of an
   * application attempt.
   * 
   * @param containers
   *          a list of <code>ContainerReport</code> for all the containers of
   *          an application attempt
   * 
   */
  @Public
  @Unstable
  public abstract void setContainerList(List<ContainerReport> containers);
}
