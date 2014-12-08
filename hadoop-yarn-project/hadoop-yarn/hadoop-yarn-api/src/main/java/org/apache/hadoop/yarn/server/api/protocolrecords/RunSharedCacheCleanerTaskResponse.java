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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <p>
 * The response to admin from the <code>SharedCacheManager</code> when
 * is asked to run the cleaner service.
 * </p>
 * 
 * <p>
 * Currently, this is empty.
 * </p>
 */
@Public
@Unstable
public abstract class RunSharedCacheCleanerTaskResponse {

  /**
   * Get whether or not the shared cache manager has accepted the request.
   * Shared cache manager will reject the request if there is an ongoing task
   * 
   * @return boolean True if the request has been accepted, false otherwise.
   */
  @Public
  @Unstable
  public abstract boolean getAccepted();

  /**
   * Set whether or not the shared cache manager has accepted the request Shared
   * cache manager will reject the request if there is an ongoing task
   * 
   * @param b True if the request has been accepted, false otherwise.
   */
  @Public
  @Unstable
  public abstract void setAccepted(boolean b);

}
