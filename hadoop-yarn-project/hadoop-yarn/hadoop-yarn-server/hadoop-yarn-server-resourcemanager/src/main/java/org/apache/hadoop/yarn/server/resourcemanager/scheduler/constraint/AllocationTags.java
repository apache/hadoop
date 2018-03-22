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
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Set;

/**
 * Allocation tags under same namespace.
 */
public final class AllocationTags {

  private AllocationTagNamespace ns;
  private Set<String> tags;

  private AllocationTags(AllocationTagNamespace namespace,
      Set<String> allocationTags) {
    this.ns = namespace;
    this.tags = allocationTags;
  }

  /**
   * @return the namespace of these tags.
   */
  public AllocationTagNamespace getNamespace() {
    return this.ns;
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
    AllocationTagNamespace namespace = new AllocationTagNamespace.AppID(appId);
    return new AllocationTags(namespace, tags);
  }

  @VisibleForTesting
  public static AllocationTags createGlobalAllocationTags(Set<String> tags) {
    AllocationTagNamespace namespace = new AllocationTagNamespace.All();
    return new AllocationTags(namespace, tags);
  }

  @VisibleForTesting
  public static AllocationTags createOtherAppAllocationTags(
      ApplicationId currentApp, Set<ApplicationId> allIds, Set<String> tags)
      throws InvalidAllocationTagsQueryException {
    AllocationTagNamespace namespace = new AllocationTagNamespace.NotSelf();
    TargetApplications ta = new TargetApplications(currentApp, allIds);
    namespace.evaluate(ta);
    return new AllocationTags(namespace, tags);
  }

  public static AllocationTags newAllocationTags(
      AllocationTagNamespace namespace, Set<String> tags) {
    return new AllocationTags(namespace, tags);
  }
}