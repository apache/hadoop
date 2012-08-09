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
import org.apache.hadoop.yarn.api.ContainerManager;

/**
 * <p><code>LocalResource</code> represents a local resource required to
 * run a container.</p>
 * 
 * <p>The <code>NodeManager</code> is responsible for localizing the resource 
 * prior to launching the container.</p>
 * 
 * <p>Applications can specify {@link LocalResourceType} and 
 * {@link LocalResourceVisibility}.</p>
 * 
 * @see LocalResourceType
 * @see LocalResourceVisibility
 * @see ContainerLaunchContext
 * @see ApplicationSubmissionContext
 * @see ContainerManager#startContainer(org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest)
 */
@Public
@Stable
public interface LocalResource {
  /**
   * Get the <em>location</em> of the resource to be localized.
   * @return <em>location</em> of the resource to be localized
   */
  public URL getResource();
  
  /**
   * Set <em>location</em> of the resource to be localized.
   * @param resource <em>location</em> of the resource to be localized
   */
  public void setResource(URL resource);
  
  /**
   * Get the <em>size</em> of the resource to be localized.
   * @return <em>size</em> of the resource to be localized
   */
  public long getSize();
  
  /**
   * Set the <em>size</em> of the resource to be localized.
   * @param size <em>size</em> of the resource to be localized
   */
  public void setSize(long size);
  
  /**
   * Get the original <em>timestamp</em> of the resource to be localized, used
   * for verification.
   * @return <em>timestamp</em> of the resource to be localized
   */
  public long getTimestamp();
  
  /**
   * Set the <em>timestamp</em> of the resource to be localized, used
   * for verification.
   * @param timestamp <em>timestamp</em> of the resource to be localized
   */
  public void setTimestamp(long timestamp);
  
  /**
   * Get the <code>LocalResourceType</code> of the resource to be localized.
   * @return <code>LocalResourceType</code> of the resource to be localized
   */
  public LocalResourceType getType();
  
  /**
   * Set the <code>LocalResourceType</code> of the resource to be localized.
   * @param type <code>LocalResourceType</code> of the resource to be localized
   */
  public void setType(LocalResourceType type);
  
  /**
   * Get the <code>LocalResourceVisibility</code> of the resource to be 
   * localized.
   * @return <code>LocalResourceVisibility</code> of the resource to be 
   *         localized
   */
  public LocalResourceVisibility getVisibility();
  
  /**
   * Set the <code>LocalResourceVisibility</code> of the resource to be 
   * localized.
   * @param visibility <code>LocalResourceVisibility</code> of the resource to be 
   *                   localized
   */
  public void setVisibility(LocalResourceVisibility visibility);
}
