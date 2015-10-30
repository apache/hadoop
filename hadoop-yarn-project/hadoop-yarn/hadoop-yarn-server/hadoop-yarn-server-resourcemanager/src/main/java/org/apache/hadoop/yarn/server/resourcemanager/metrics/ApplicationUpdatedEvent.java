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

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;

public class ApplicationUpdatedEvent extends SystemMetricsEvent {

  private ApplicationId appId;
  private String queue;
  private Priority applicationPriority;

  public ApplicationUpdatedEvent(ApplicationId appId, String queue,
      long updatedTime, Priority applicationPriority) {
    super(SystemMetricsEventType.APP_UPDATED, updatedTime);
    this.appId = appId;
    this.queue = queue;
    this.applicationPriority = applicationPriority;
  }

  @Override
  public int hashCode() {
    return appId.hashCode();
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public String getQueue() {
    return queue;
  }

  public Priority getApplicationPriority() {
    return applicationPriority;
  }
}