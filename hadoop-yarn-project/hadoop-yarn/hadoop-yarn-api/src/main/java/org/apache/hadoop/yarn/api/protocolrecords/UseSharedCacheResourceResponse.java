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

/**
 * <p>
 * The response from the SharedCacheManager to the client that indicates whether
 * a requested resource exists in the cache.
 * </p>
 */
@Public
@Unstable
public abstract class UseSharedCacheResourceResponse {

  /**
   * Get the <code>Path</code> corresponding to the requested resource in the
   * shared cache.
   *
   * @return String A <code>Path</code> if the resource exists in the shared
   *         cache, <code>null</code> otherwise
   */
  @Public
  @Unstable
  public abstract String getPath();

  /**
   * Set the <code>Path</code> corresponding to a resource in the shared cache.
   *
   * @param p A <code>Path</code> corresponding to a resource in the shared
   *          cache
   */
  @Public
  @Unstable
  public abstract void setPath(String p);

}
