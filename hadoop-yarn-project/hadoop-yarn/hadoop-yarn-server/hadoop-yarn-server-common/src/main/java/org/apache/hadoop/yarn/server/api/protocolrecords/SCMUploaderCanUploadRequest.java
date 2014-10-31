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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <p>
 * The request from the NodeManager to the <code>SharedCacheManager</code> that
 * requests whether it can upload a resource in the shared cache.
 * </p>
 */
@Private
@Unstable
public abstract class SCMUploaderCanUploadRequest {

  /**
   * Get the <code>key</code> of the resource that would be uploaded to the
   * shared cache.
   *
   * @return <code>key</code>
   */
  public abstract String getResourceKey();

  /**
   * Set the <code>key</code> of the resource that would be uploaded to the
   * shared cache.
   *
   * @param key unique identifier for the resource
   */
  public abstract void setResourceKey(String key);
}
