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

package org.apache.slider.server.appmaster.state;

import org.apache.hadoop.yarn.api.records.Container;

/**
 * Static assignment structure
 */
public class ContainerAssignment {

  /**
   * Container that has been allocated
   */
  public final Container container;

  /**
   * Role to assign to it
   */
  public final RoleStatus role;

  /**
   * Placement outcome: was this from history or not
   */
  public final ContainerAllocationOutcome placement;

  public ContainerAssignment(Container container,
      RoleStatus role,
      ContainerAllocationOutcome placement) {
    this.container = container;
    this.role = role;
    this.placement = placement;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ContainerAssignment{");
    sb.append("container=").append(container);
    sb.append(", role=").append(role);
    sb.append(", placement=").append(placement);
    sb.append('}');
    return sb.toString();
  }
}
