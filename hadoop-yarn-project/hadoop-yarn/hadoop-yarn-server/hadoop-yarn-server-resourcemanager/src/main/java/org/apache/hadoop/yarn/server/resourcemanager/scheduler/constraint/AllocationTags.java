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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Set;

/**
 * Allocation tags under same namespace.
 */
public final class AllocationTags {

  private TargetApplicationsNamespace ns;
  private Set<String> tags;
  private ApplicationId applicationId;

  private AllocationTags(TargetApplicationsNamespace namespace,
      Set<String> allocationTags) {
    this.ns = namespace;
    this.tags = allocationTags;
  }

  private AllocationTags(TargetApplicationsNamespace namespace,
      Set<String> allocationTags, ApplicationId currentAppId) {
    this.ns = namespace;
    this.tags = allocationTags;
    this.applicationId = currentAppId;
  }

  /**
   * @return the namespace of these tags.
   */
  public TargetApplicationsNamespace getNamespace() {
    return this.ns;
  }

  public ApplicationId getCurrentApplicationId() {
    return this.applicationId;
  }

  /**
   * @return the allocation tags.
   */
  public Set<String> getTags() {
    return this.tags;
  }

  @VisibleForTesting
  public static AllocationTags createSingleAppAllocationTags(
      ApplicationId appId, Set<String> tags) {
    TargetApplicationsNamespace namespace =
        new TargetApplicationsNamespace.AppID(appId);
    return new AllocationTags(namespace, tags);
  }

  @VisibleForTesting
  public static AllocationTags createGlobalAllocationTags(Set<String> tags) {
    TargetApplicationsNamespace namespace =
        new TargetApplicationsNamespace.All();
    return new AllocationTags(namespace, tags);
  }

  @VisibleForTesting
  public static AllocationTags createOtherAppAllocationTags(
      ApplicationId currentApp, Set<String> tags) {
    TargetApplicationsNamespace namespace =
        new TargetApplicationsNamespace.NotSelf();
    return new AllocationTags(namespace, tags, currentApp);
  }

  public static AllocationTags createAllocationTags(
      ApplicationId currentApplicationId, String namespaceString,
      Set<String> tags) throws InvalidAllocationTagsQueryException {
    TargetApplicationsNamespace namespace = TargetApplicationsNamespace
        .parse(namespaceString);
    return new AllocationTags(namespace, tags, currentApplicationId);
  }
}