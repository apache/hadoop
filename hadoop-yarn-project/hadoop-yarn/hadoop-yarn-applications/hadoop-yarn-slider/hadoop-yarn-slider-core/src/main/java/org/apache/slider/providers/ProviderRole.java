/*
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

package org.apache.slider.providers;

import org.apache.slider.api.ResourceKeys;

/**
 * Provider role and key for use in app requests.
 * 
 * This class uses the role name as the key for hashes and in equality tests,
 * and ignores the other values.
 */
public final class ProviderRole {
  public final String name;
  public final String group;
  public final int id;
  public int placementPolicy;
  public int nodeFailureThreshold;
  public final long placementTimeoutSeconds;
  public final String labelExpression;

  public ProviderRole(String name, int id) {
    this(name,
        name,
        id,
        PlacementPolicy.DEFAULT,
        ResourceKeys.DEFAULT_NODE_FAILURE_THRESHOLD,
        ResourceKeys.DEFAULT_PLACEMENT_ESCALATE_DELAY_SECONDS,
        ResourceKeys.DEF_YARN_LABEL_EXPRESSION);
  }

  /**
   * Create a provider role
   * @param name role/component name
   * @param id ID. This becomes the YARN priority
   * @param policy placement policy
   * @param nodeFailureThreshold threshold for node failures (within a reset interval)
   * after which a node failure is considered an app failure
   * @param placementTimeoutSeconds for lax placement, timeout in seconds before
   * @param labelExpression label expression for requests; may be null
   */
  public ProviderRole(String name,
      int id,
      int policy,
      int nodeFailureThreshold,
      long placementTimeoutSeconds,
      String labelExpression) {
    this(name,
        name,
        id,
        policy,
        nodeFailureThreshold,
        placementTimeoutSeconds,
        labelExpression);
  }

  /**
   * Create a provider role with a role group
   * @param name role/component name
   * @param group role/component group
   * @param id ID. This becomes the YARN priority
   * @param policy placement policy
   * @param nodeFailureThreshold threshold for node failures (within a reset interval)
   * after which a node failure is considered an app failure
   * @param placementTimeoutSeconds for lax placement, timeout in seconds before
   * @param labelExpression label expression for requests; may be null
   */
  public ProviderRole(String name,
      String group,
      int id,
      int policy,
      int nodeFailureThreshold,
      long placementTimeoutSeconds,
      String labelExpression) {
    this.name = name;
    if (group == null) {
      this.group = name;
    } else {
      this.group = group;
    }
    this.id = id;
    this.placementPolicy = policy;
    this.nodeFailureThreshold = nodeFailureThreshold;
    this.placementTimeoutSeconds = placementTimeoutSeconds;
    this.labelExpression = labelExpression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProviderRole that = (ProviderRole) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ProviderRole{");
    sb.append("name='").append(name).append('\'');
    sb.append(", group=").append(group);
    sb.append(", id=").append(id);
    sb.append(", placementPolicy=").append(placementPolicy);
    sb.append(", nodeFailureThreshold=").append(nodeFailureThreshold);
    sb.append(", placementTimeoutSeconds=").append(placementTimeoutSeconds);
    sb.append(", labelExpression='").append(labelExpression).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
