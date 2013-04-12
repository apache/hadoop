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
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

/**
 * <p>The request sent by a client to the <code>ResourceManager</code> to 
 * get an {@link ApplicationReport} for an application.</p>
 * 
 * <p>The request should include the {@link ApplicationId} of the 
 * application.</p>
 * 
 * @see ClientRMProtocol#getApplicationReport(GetApplicationReportRequest)
 * @see ApplicationReport
 */
@Public
@Stable
public interface GetApplicationReportRequest {
  /**
   * Get the <code>ApplicationId</code> of the application.
   * @return <code>ApplicationId</code> of the application
   */
  public ApplicationId getApplicationId();
  
  /**
   * Set the <code>ApplicationId</code> of the application
   * @param applicationId <code>ApplicationId</code> of the application
   */
  public void setApplicationId(ApplicationId applicationId);
}
