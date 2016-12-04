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
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * {@code AbstractResourceRequest} represents a generic resource request made
 * by an application to the {@code ResourceManager}.
 * <p>
 * It includes:
 * <ul>
 *   <li>{@link Resource} capability required for each request.</li>
 * </ul>
 *
 * @see Resource
 */
@Public
@Unstable
public abstract class AbstractResourceRequest {

  /**
   * Set the <code>Resource</code> capability of the request
   * @param capability <code>Resource</code> capability of the request
   */
  @Public
  @Stable
  public abstract void setCapability(Resource capability);

  /**
   * Get the <code>Resource</code> capability of the request.
   * @return <code>Resource</code> capability of the request
   */
  @Public
  @Stable
  public abstract Resource getCapability();
}
