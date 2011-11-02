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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;

/**
 * <p><code>ApplicationSubmissionContext</code> represents the all of the 
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
   * @param appplicationId <code>ApplicationId</code> of the submitted 
   *                       application
   */
  @Public
  @Stable
  public void setApplicationId(ApplicationId appplicationId);

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
   * Get the <em>user</em> submitting the application.
   * @return <em>user</em> submitting the application
   */
  @Public
  @Stable
  public String getUser();
  
  /**
   * Set the <em>user</em> submitting the application.
   * @param user <em>user</em> submitting the application
   */
  @Public
  @Stable
  public void setUser(String user);

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
}