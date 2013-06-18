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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@link ResourceBlacklistRequest} encapsulates the list of resource-names 
 * which should be added or removed from the <em>blacklist</em> of resources 
 * for the application.
 * 
 * @see ResourceRequest
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */
@Public
@Stable
public abstract class ResourceBlacklistRequest {

  @Public
  @Stable
  public static ResourceBlacklistRequest newInstance(
      List<String> additions, List<String> removals) {
    ResourceBlacklistRequest blacklistRequest = 
        Records.newRecord(ResourceBlacklistRequest.class);
    blacklistRequest.setBlacklistAdditions(additions);
    blacklistRequest.setBlacklistRemovals(removals);
    return blacklistRequest;
  }
  
  /**
   * Get the list of resource-names which should be added to the 
   * application blacklist.
   * 
   * @return list of resource-names which should be added to the 
   *         application blacklist
   */
  @Public
  @Stable
  public abstract List<String> getBlacklistAdditions();
  
  /**
   * Set list of resource-names which should be added to the application blacklist.
   * 
   * @param resourceNames list of resource-names which should be added to the 
   *                  application blacklist
   */
  @Public
  @Stable
  public abstract void setBlacklistAdditions(List<String> resourceNames);
  
  /**
   * Get the list of resource-names which should be removed from the 
   * application blacklist.
   * 
   * @return list of resource-names which should be removed from the 
   *         application blacklist
   */
  @Public
  @Stable
  public abstract List<String> getBlacklistRemovals();
  
  /**
   * Set list of resource-names which should be removed from the 
   * application blacklist.
   * 
   * @param resourceNames list of resource-names which should be removed from the 
   *                  application blacklist
   */
  @Public
  @Stable
  public abstract void setBlacklistRemovals(List<String> resourceNames);

}
