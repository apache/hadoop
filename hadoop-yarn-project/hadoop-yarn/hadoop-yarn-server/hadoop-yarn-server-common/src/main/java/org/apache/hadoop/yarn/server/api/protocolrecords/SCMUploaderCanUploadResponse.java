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
 * The response from the SharedCacheManager to the NodeManager that indicates
 * whether the NodeManager can upload the resource to the shared cache. If it is
 * not accepted by SCM, the NodeManager should not upload it to the shared
 * cache.
 * </p>
 */
@Private
@Unstable
public abstract class SCMUploaderCanUploadResponse {

  /**
   * Get whether or not the node manager can upload the resource to the shared
   * cache.
   *
   * @return boolean True if the resource can be uploaded, false otherwise.
   */
  public abstract boolean getUploadable();

  /**
   * Set whether or not the node manager can upload the resource to the shared
   * cache.
   *
   * @param b True if the resource can be uploaded, false otherwise.
   */
  public abstract void setUploadable(boolean b);

}
