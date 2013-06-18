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
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by a client to <em>submit an application</em> to the 
 * <code>ResourceManager</code>.</p>
 * 
 * <p>The request, via {@link ApplicationSubmissionContext}, contains
 * details such as queue, {@link Resource} required to run the 
 * <code>ApplicationMaster</code>, the equivalent of 
 * {@link ContainerLaunchContext} for launching the 
 * <code>ApplicationMaster</code> etc.
 * 
 * @see ApplicationClientProtocol#submitApplication(SubmitApplicationRequest)
 */
@Public
@Stable
public abstract class SubmitApplicationRequest {

  @Public
  @Stable
  public static SubmitApplicationRequest newInstance(
      ApplicationSubmissionContext context) {
    SubmitApplicationRequest request =
        Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(context);
    return request;
  }

  /**
   * Get the <code>ApplicationSubmissionContext</code> for the application.
   * @return <code>ApplicationSubmissionContext</code> for the application
   */
  @Public
  @Stable
  public abstract ApplicationSubmissionContext getApplicationSubmissionContext();

  /**
   * Set the <code>ApplicationSubmissionContext</code> for the application.
   * @param context <code>ApplicationSubmissionContext</code> for the 
   *                application
   */
  @Public
  @Stable
  public abstract void setApplicationSubmissionContext(
      ApplicationSubmissionContext context);
}
