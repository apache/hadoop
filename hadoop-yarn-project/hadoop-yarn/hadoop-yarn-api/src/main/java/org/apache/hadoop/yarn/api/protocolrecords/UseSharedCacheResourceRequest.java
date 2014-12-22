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
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * <p>
 * The request from clients to the <code>SharedCacheManager</code> that claims a
 * resource in the shared cache.
 * </p>
 */
@Public
@Unstable
public abstract class UseSharedCacheResourceRequest {

  /**
   * Get the <code>ApplicationId</code> of the resource to be used.
   *
   * @return <code>ApplicationId</code>
   */
  @Public
  @Unstable
  public abstract ApplicationId getAppId();

  /**
   * Set the <code>ApplicationId</code> of the resource to be used.
   *
   * @param id <code>ApplicationId</code>
   */
  @Public
  @Unstable
  public abstract void setAppId(ApplicationId id);

  /**
   * Get the <code>key</code> of the resource to be used.
   *
   * @return <code>key</code>
   */
  @Public
  @Unstable
  public abstract String getResourceKey();

  /**
   * Set the <code>key</code> of the resource to be used.
   *
   * @param key unique identifier for the resource
   */
  @Public
  @Unstable
  public abstract void setResourceKey(String key);
}
