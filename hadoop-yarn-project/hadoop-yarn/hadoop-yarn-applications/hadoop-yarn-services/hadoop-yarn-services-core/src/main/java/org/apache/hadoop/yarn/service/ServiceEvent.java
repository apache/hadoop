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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.service.api.records.Component;

import java.util.List;

/**
 * Events are handled by {@link ServiceManager} to manage the service
 * state.
 */
public class ServiceEvent extends AbstractEvent<ServiceEventType> {

  private final ServiceEventType type;
  private String version;
  private boolean autoFinalize;
  private boolean expressUpgrade;
  // For express upgrade they should be in order.
  private List<Component> compsToUpgrade;

  public ServiceEvent(ServiceEventType serviceEventType) {
    super(serviceEventType);
    this.type = serviceEventType;
  }

  public ServiceEventType getType() {
    return type;
  }

  public String getVersion() {
    return version;
  }

  public ServiceEvent setVersion(String version) {
    this.version = version;
    return this;
  }

  public boolean isAutoFinalize() {
    return autoFinalize;
  }

  public ServiceEvent setAutoFinalize(boolean autoFinalize) {
    this.autoFinalize = autoFinalize;
    return this;
  }

  public boolean isExpressUpgrade() {
    return expressUpgrade;
  }

  public ServiceEvent setExpressUpgrade(boolean expressUpgrade) {
    this.expressUpgrade = expressUpgrade;
    return this;
  }

  public List<Component> getCompsToUpgrade() {
    return compsToUpgrade;
  }

  public ServiceEvent setCompsToUpgrade(List<Component> compsToUpgrade) {
    this.compsToUpgrade = compsToUpgrade;
    return this;
  }

}
