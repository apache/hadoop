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
 * notifies that a resource has been uploaded to the shared cache. The
 * <code>SharedCacheManager</code> may reject the resource for various reasons,
 * in which case the NodeManager should remove it from the shared cache.
 * </p>
 */
@Private
@Unstable
public abstract class SCMUploaderNotifyRequest {

  /**
   * Get the filename of the resource that was just uploaded to the shared
   * cache.
   *
   * @return the filename
   */
  public abstract String getFileName();

  /**
   * Set the filename of the resource that was just uploaded to the shared
   * cache.
   *
   * @param filename the name of the file
   */
  public abstract void setFilename(String filename);

  /**
   * Get the <code>key</code> of the resource that was just uploaded to the
   * shared cache.
   *
   * @return <code>key</code>
   */
  public abstract String getResourceKey();

  /**
   * Set the <code>key</code> of the resource that was just uploaded to the
   * shared cache.
   *
   * @param key unique identifier for the resource
   */
  public abstract void setResourceKey(String key);
}
