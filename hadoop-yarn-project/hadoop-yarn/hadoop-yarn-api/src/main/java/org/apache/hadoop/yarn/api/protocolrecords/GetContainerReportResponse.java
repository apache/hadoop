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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a client requesting
 * a container report.
 * </p>
 * 
 * <p>
 * The response includes a {@link ContainerReport} which has details of a
 * container.
 * </p>
 * 
 */
@Public
@Unstable
public abstract class GetContainerReportResponse {
  @Public
  @Unstable
  public static GetContainerReportResponse newInstance(
      ContainerReport containerReport) {
    GetContainerReportResponse response =
        Records.newRecord(GetContainerReportResponse.class);
    response.setContainerReport(containerReport);
    return response;
  }

  /**
   * Get the <code>ContainerReport</code> for the container.
   * 
   * @return <code>ContainerReport</code> for the container
   */
  @Public
  @Unstable
  public abstract ContainerReport getContainerReport();

  @Public
  @Unstable
  public abstract void setContainerReport(ContainerReport containerReport);
}
