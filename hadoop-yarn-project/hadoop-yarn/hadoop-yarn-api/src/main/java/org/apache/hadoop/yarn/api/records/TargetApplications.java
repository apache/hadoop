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

import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is used by
 * {@link AllocationTagNamespace#evaluate(TargetApplications)} to evaluate
 * a namespace.
 */
public class TargetApplications {

  private ApplicationId currentAppId;
  private Set<ApplicationId> allAppIds;

  public TargetApplications(ApplicationId currentApplicationId,
      Set<ApplicationId> allApplicationIds) {
    this.currentAppId = currentApplicationId;
    this.allAppIds = allApplicationIds;
  }

  public Set<ApplicationId> getAllApplicationIds() {
    return this.allAppIds;
  }

  public ApplicationId getCurrentApplicationId() {
    return this.currentAppId;
  }

  public Set<ApplicationId> getOtherApplicationIds() {
    return allAppIds == null ? null : allAppIds.stream().filter(appId ->
        !appId.equals(getCurrentApplicationId()))
        .collect(Collectors.toSet());
  }
}
