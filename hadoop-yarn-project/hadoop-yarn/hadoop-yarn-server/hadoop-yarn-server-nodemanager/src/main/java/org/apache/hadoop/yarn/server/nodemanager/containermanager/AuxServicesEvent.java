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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container
    .Container;

public class AuxServicesEvent extends AbstractEvent<AuxServicesEventType> {

  private final String user;
  private final String serviceId;
  private final ByteBuffer serviceData;
  private final ApplicationId appId;
  private final Container container;

  public AuxServicesEvent(AuxServicesEventType eventType, ApplicationId appId) {
    this(eventType, null, appId, null, null);
  }

  public AuxServicesEvent(AuxServicesEventType eventType, Container container) {
    this(eventType, null, container.getContainerId().getApplicationAttemptId()
        .getApplicationId(), null, null, container);
  }

  public AuxServicesEvent(AuxServicesEventType eventType, String user,
      ApplicationId appId, String serviceId, ByteBuffer serviceData) {
    this(eventType, user, appId, serviceId, serviceData, null);
  }
    public AuxServicesEvent(AuxServicesEventType eventType, String user,
      ApplicationId appId, String serviceId, ByteBuffer serviceData,
        Container container) {
    super(eventType);
    this.user = user;
    this.appId = appId;
    this.serviceId = serviceId;
    this.serviceData = serviceData;
    this.container = container;
  }

  public String getServiceID() {
    return serviceId;
  }

  public ByteBuffer getServiceData() {
    return serviceData;
  }

  public String getUser() {
    return user;
  }

  public ApplicationId getApplicationID() {
    return appId;
  }

  public Container getContainer() {
    return container;
  }

}
