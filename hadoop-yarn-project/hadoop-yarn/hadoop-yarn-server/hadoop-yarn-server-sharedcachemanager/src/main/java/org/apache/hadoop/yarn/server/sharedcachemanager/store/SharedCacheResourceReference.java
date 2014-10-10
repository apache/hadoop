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

package org.apache.hadoop.yarn.server.sharedcachemanager.store;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * This is an object that represents a reference to a shared cache resource.
 */
@Private
@Evolving
public class SharedCacheResourceReference {
  private final ApplicationId appId;
  private final String shortUserName;

  /**
   * Create a resource reference.
   * 
   * @param appId <code>ApplicationId</code> that is referencing a resource.
   * @param shortUserName <code>ShortUserName</code> of the user that created
   *          the reference.
   */
  public SharedCacheResourceReference(ApplicationId appId, String shortUserName) {
    this.appId = appId;
    this.shortUserName = shortUserName;
  }

  public ApplicationId getAppId() {
    return this.appId;
  }

  public String getShortUserName() {
    return this.shortUserName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((appId == null) ? 0 : appId.hashCode());
    result =
        prime * result
            + ((shortUserName == null) ? 0 : shortUserName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SharedCacheResourceReference other = (SharedCacheResourceReference) obj;
    if (appId == null) {
      if (other.appId != null)
        return false;
    } else if (!appId.equals(other.appId))
      return false;
    if (shortUserName == null) {
      if (other.shortUserName != null)
        return false;
    } else if (!shortUserName.equals(other.shortUserName))
      return false;
    return true;
  }
}