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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;

/**
 * <p><code>ApplicationSubmissionContext</code> represents the all of the 
 * information needed by the <code>ResourceManager</code> to launch 
 * the <code>ApplicationMaster</code> for an application.</p>
 * 
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link ApplicationId} of the application.</li>
 *     <li>
 *       {@link Resource} necessary to run the <code>ApplicationMaster</code>.
 *     </li>
 *     <li>Application user.</li>
 *     <li>Application name.</li>
 *     <li>{@link Priority} of the application.</li>
 *     <li>Security tokens (if security is enabled).</li>
 *     <li>
 *       {@link LocalResource} necessary for running the 
 *       <code>ApplicationMaster</code> container such
 *       as binaries, jar, shared-objects, side-files etc. 
 *     </li>
 *     <li>
 *       Environment variables for the launched <code>ApplicationMaster</code> 
 *       process.
 *     </li>
 *     <li>Command to launch the <code>ApplicationMaster</code>.</li>
 *   </ul>
 * </p>
 * 
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
   * Get the <code>Resource</code> required to run the 
   * <code>ApplicationMaster</code>.
   * @return <code>Resource</code> required to run the 
   *         <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public Resource getMasterCapability();
  
  /**
   * Set <code>Resource</code> required to run the 
   * <code>ApplicationMaster</code>.
   * @param masterCapability <code>Resource</code> required to run the 
   *                         <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public void setMasterCapability(Resource masterCapability);
  
  @Private
  @Unstable
  public Map<String, URL> getAllResources();
  
  @Private
  @Unstable
  public URL getResource(String key);
  
  @Private
  @Unstable
  public void addAllResources(Map<String, URL> resources);

  @Private
  @Unstable
  public void setResource(String key, URL url);

  @Private
  @Unstable
  public void removeResource(String key);

  @Private
  @Unstable
  public void clearResources();

  /**
   * Get all the <code>LocalResource</code> required to run the 
   * <code>ApplicationMaster</code>.
   * @return <code>LocalResource</code> required to run the 
   *         <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public Map<String, LocalResource> getAllResourcesTodo();
  
  @Private
  @Unstable
  public LocalResource getResourceTodo(String key);
  
  /**
   * Add all the <code>LocalResource</code> required to run the 
   * <code>ApplicationMaster</code>.
   * @param resources all <code>LocalResource</code> required to run the 
   *                      <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public void addAllResourcesTodo(Map<String, LocalResource> resources);

  @Private
  @Unstable
  public void setResourceTodo(String key, LocalResource localResource);

  @Private
  @Unstable
  public void removeResourceTodo(String key);

  @Private
  @Unstable
  public void clearResourcesTodo();

  @Private
  @Unstable
  public List<String> getFsTokenList();
  
  @Private
  @Unstable
  public String getFsToken(int index);
  
  @Private
  @Unstable
  public int getFsTokenCount();
  
  @Private
  @Unstable
  public void addAllFsTokens(List<String> fsTokens);

  @Private
  @Unstable
  public void addFsToken(String fsToken);

  @Private
  @Unstable
  public void removeFsToken(int index);

  @Private
  @Unstable
  public void clearFsTokens();

  /**
   * Get <em>file-system tokens</em> for the <code>ApplicationMaster</code>.
   * @return file-system tokens for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public ByteBuffer getFsTokensTodo();
  
  /**
   * Set <em>file-system tokens</em> for the <code>ApplicationMaster</code>.
   * @param fsTokens file-system tokens for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public void setFsTokensTodo(ByteBuffer fsTokens);

  /**
   * Get the <em>environment variables</em> for the 
   * <code>ApplicationMaster</code>.
   * @return environment variables for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public Map<String, String> getAllEnvironment();
  
  @Private
  @Unstable
  public String getEnvironment(String key);
  
  /**
   * Add all of the <em>environment variables</em> for the 
   * <code>ApplicationMaster</code>.
   * @param environment environment variables for the 
   *                    <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public void addAllEnvironment(Map<String, String> environment);

  @Private
  @Unstable
  public void setEnvironment(String key, String env);

  @Private
  @Unstable
  public void removeEnvironment(String key);

  @Private
  @Unstable
  public void clearEnvironment();

  /**
   * Get the <em>commands</em> to launch the <code>ApplicationMaster</code>.
   * @return commands to launch the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public List<String> getCommandList();
  
  @Private
  @Unstable
  public String getCommand(int index);
  
  @Private
  @Unstable
  public int getCommandCount();
  
  /**
   * Add all of the <em>commands</em> to launch the 
   * <code>ApplicationMaster</code>.
   * @param commands commands to launch the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public void addAllCommands(List<String> commands);
  
  @Private
  @Unstable
  public void addCommand(String command);
  
  @Private
  @Unstable
  public void removeCommand(int index);
  
  @Private
  @Unstable
  public void clearCommands();
}