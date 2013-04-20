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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;

/**
 * <p><code>ApplicationSubmissionContext</code> represents all of the
 * information needed by the <code>ResourceManager</code> to launch 
 * the <code>ApplicationMaster</code> for an application.</p>
 * 
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link ApplicationId} of the application.</li>
 *     <li>Application user.</li>
 *     <li>Application name.</li>
 *     <li>{@link Priority} of the application.</li>
 *     <li>
 *       {@link ContainerLaunchContext} of the container in which the 
 *       <code>ApplicationMaster</code> is executed.
 *     </li>
 *   </ul>
 * </p>
 * 
 * @see ContainerLaunchContext
 * @see ClientRMProtocol#submitApplication(org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest)
 */
@Public
@Stable
public interface ApplicationSubmissionContext {
  /**
   * Get the <code>ApplicationId</code> of the submitted application.
   * @return <code>ApplicationId</code> of the submitted application
   */
  @Public
  @Stable
  public ApplicationId getApplicationId();
  
  /**
   * Set the <code>ApplicationId</code> of the submitted application.
   * @param applicationId <code>ApplicationId</code> of the submitted
   *                      application
   */
  @Public
  @Stable
  public void setApplicationId(ApplicationId applicationId);

  /**
   * Get the application <em>name</em>.
   * @return application name
   */
  @Public
  @Stable
  public String getApplicationName();
  
  /**
   * Set the application <em>name</em>.
   * @param applicationName application name
   */
  @Public
  @Stable
  public void setApplicationName(String applicationName);
  
  /**
   * Get the <em>queue</em> to which the application is being submitted.
   * @return <em>queue</em> to which the application is being submitted
   */
  @Public
  @Stable
  public String getQueue();
  
  /**
   * Set the <em>queue</em> to which the application is being submitted
   * @param queue <em>queue</em> to which the application is being submitted
   */
  @Public
  @Stable
  public void setQueue(String queue);
  
  /**
   * Get the <code>Priority</code> of the application.
   * @return <code>Priority</code> of the application
   */
  @Public
  @Stable
  public Priority getPriority();

  /**
   * Set the <code>Priority</code> of the application.
   * @param priority <code>Priority</code> of the application
   */
  @Public
  @Stable
  public void setPriority(Priority priority);

  /**
   * Get the <code>ContainerLaunchContext</code> to describe the 
   * <code>Container</code> with which the <code>ApplicationMaster</code> is
   * launched.
   * @return <code>ContainerLaunchContext</code> for the 
   *         <code>ApplicationMaster</code> container
   */
  @Public
  @Stable
  public ContainerLaunchContext getAMContainerSpec();
  
  /**
   * Set the <code>ContainerLaunchContext</code> to describe the 
   * <code>Container</code> with which the <code>ApplicationMaster</code> is
   * launched.
   * @param amContainer <code>ContainerLaunchContext</code> for the 
   *                    <code>ApplicationMaster</code> container
   */
  @Public
  @Stable
  public void setAMContainerSpec(ContainerLaunchContext amContainer);
  
  /**
   * Get if the RM should manage the execution of the AM. 
   * If true, then the RM 
   * will not allocate a container for the AM and start it. It will expect the 
   * AM to be launched and connect to the RM within the AM liveliness period and 
   * fail the app otherwise. The client should launch the AM only after the RM 
   * has ACCEPTED the application and changed the <code>YarnApplicationState</code>.
   * Such apps will not be retried by the RM on app attempt failure.
   * The default value is false.
   * @return true if the AM is not managed by the RM
   */
  @Public
  @Unstable
  public boolean getUnmanagedAM();
  
  /**
   * @param value true if RM should not manage the AM
   */
  @Public
  @Unstable
  public void setUnmanagedAM(boolean value);

  /**
   * @return true if tokens should be canceled when the app completes.
   */
  @LimitedPrivate("mapreduce")
  @Unstable
  public boolean getCancelTokensWhenComplete();
  
  /**
   * Set to false if tokens should not be canceled when the app finished else
   * false.  WARNING: this is not recommended unless you want your single job
   * tokens to be reused by others jobs.
   * @param cancel true if tokens should be canceled when the app finishes. 
   */
  @LimitedPrivate("mapreduce")
  @Unstable
  public void setCancelTokensWhenComplete(boolean cancel);

  /**
   * @return the number of max attempts of the application to be submitted
   */
  @Public
  @Unstable
  public int getMaxAppAttempts();

  /**
   * Set the number of max attempts of the application to be submitted. WARNING:
   * it should be no larger than the global number of max attempts in the Yarn
   * configuration.
   * @param maxAppAttempts the number of max attempts of the application
   * to be submitted.
   */
  @Public
  @Unstable
  public void setMaxAppAttempts(int maxAppAttempts);

  @Public
  @Stable
  public Resource getResource();

  @Public
  @Stable
  public void setResource(Resource resource);
}