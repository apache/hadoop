/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework.ProportionalCapacityPreemptionPolicyMockFramework.parseResourceFromString;

public class ContainerSpecification {
  Priority priority;
  Resource resource = Resource.newInstance(0, 0);
  Resource pendingResource = Resource.newInstance(0, 0);
  NodeId nodeId;
  String label;
  int repeat;
  boolean reserved;
  String username;

  private ContainerSpecification(Builder builder) {
    if (builder.resource != null) {
      Resources.addTo(resource, builder.resource);
    }
    if (builder.pendingResource != null) {
      Resources.addTo(pendingResource, builder.pendingResource);
    }
    this.priority = builder.priority;
    this.nodeId = builder.nodeId;
    this.label = builder.label;
    this.repeat = builder.repeat;
    this.reserved = builder.reserved;
    this.username = builder.username;
  }

  static class Builder {
    private Priority priority;
    private Resource resource;
    private NodeId nodeId;
    private String label;
    private int repeat;
    private boolean reserved;
    private Resource pendingResource;
    private String username = "user";

    public static Builder create() {
      return new Builder();
    }

    Builder withPriority(String value) {
      this.priority = Priority.newInstance(Integer.valueOf(value));
      return this;
    }

    Builder withResource(String value) {
      this.resource = parseResourceFromString(value);
      return this;
    }

    Builder withHostname(String value) {
      this.nodeId = NodeId.newInstance(value, 1);
      return this;
    }

    Builder withLabel(String value) {
      this.label = value;
      return this;
    }

    Builder withRepeat(String repeat) {
      this.repeat = Integer.valueOf(repeat);
      return this;
    }

    Builder withReserved(String value) {
      this.reserved = Boolean.valueOf(value);
      return this;
    }

    Builder withPendingResource(String value) {
      this.pendingResource = parseResourceFromString(value);
      return this;
    }

    Builder withUsername(String value) {
      this.username = value;
      return this;
    }

    public ContainerSpecification build() {
      return new ContainerSpecification(this);
    }
  }
}
