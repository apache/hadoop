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

package org.apache.hadoop.yarn.service.component;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;

public class ComponentEvent extends AbstractEvent<ComponentEventType> {
  private long desired;
  private final String name;
  private final ComponentEventType type;
  private Container container;
  private ComponentInstance instance;
  private ContainerStatus status;
  private ContainerId containerId;
  private org.apache.hadoop.yarn.service.api.records.Component targetSpec;

  public ContainerId getContainerId() {
    return containerId;
  }

  public ComponentEvent setContainerId(ContainerId containerId) {
    this.containerId = containerId;
    return this;
  }

  public ComponentEvent(String name, ComponentEventType type) {
    super(type);
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public ComponentEventType getType() {
    return type;
  }

  public long getDesired() {
    return desired;
  }

  public ComponentEvent setDesired(long desired) {
    this.desired = desired;
    return this;
  }

  public Container getContainer() {
    return container;
  }

  public ComponentEvent setContainer(Container container) {
    this.container = container;
    return this;
  }

  public ComponentInstance getInstance() {
    return instance;
  }

  public ComponentEvent setInstance(ComponentInstance instance) {
    this.instance = instance;
    return this;
  }

  public ContainerStatus getStatus() {
    return status;
  }

  public ComponentEvent setStatus(ContainerStatus status) {
    this.status = status;
    return this;
  }

  public org.apache.hadoop.yarn.service.api.records.Component getTargetSpec() {
    return targetSpec;
  }

  public ComponentEvent setTargetSpec(
      org.apache.hadoop.yarn.service.api.records.Component targetSpec) {
    this.targetSpec = Preconditions.checkNotNull(targetSpec);
    return this;
  }
}
