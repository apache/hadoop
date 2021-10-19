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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is used by
 * {@link TargetApplicationsNamespace#evaluate(TargetApplications)} to evaluate
 * a namespace.
 */
public class TargetApplications {

  private ApplicationId currentAppId;
  private Map<ApplicationId, Set<String>> allApps;

  public TargetApplications(ApplicationId currentApplicationId,
      Set<ApplicationId> allApplicationIds) {
    this.currentAppId = currentApplicationId;
    allApps = new HashMap<>();
    if (allApplicationIds != null) {
      allApplicationIds.forEach(appId ->
          allApps.put(appId, ImmutableSet.of()));
    }
  }

  public TargetApplications(ApplicationId currentApplicationId,
      Map<ApplicationId, Set<String>> allApplicationIds) {
    this.currentAppId = currentApplicationId;
    this.allApps = allApplicationIds;
  }

  public ApplicationId getCurrentApplicationId() {
    return this.currentAppId;
  }

  public Set<ApplicationId> getAllApplicationIds() {
    return this.allApps == null ?
        ImmutableSet.of() : allApps.keySet();
  }

  public Set<ApplicationId> getOtherApplicationIds() {
    if (getAllApplicationIds() == null
        || getAllApplicationIds().isEmpty()) {
      return ImmutableSet.of();
    }
    return getAllApplicationIds()
        .stream()
        .filter(appId -> !appId.equals(getCurrentApplicationId()))
        .collect(Collectors.toSet());
  }

  public Set<ApplicationId> getApplicationIdsByTag(String applicationTag) {
    Set<ApplicationId> result = new HashSet<>();
    if (Strings.isNullOrEmpty(applicationTag)
        || this.allApps == null) {
      return result;
    }

    for (Map.Entry<ApplicationId, Set<String>> app
        : this.allApps.entrySet()) {
      if (app.getValue() != null
          && app.getValue().contains(applicationTag)) {
        result.add(app.getKey());
      }
    }

    return result;
  }
}
