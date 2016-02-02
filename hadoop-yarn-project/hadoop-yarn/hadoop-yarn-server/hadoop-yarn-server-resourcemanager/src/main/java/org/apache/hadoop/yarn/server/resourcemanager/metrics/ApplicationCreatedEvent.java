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

import java.util.Set;

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;

public class ApplicationCreatedEvent extends
    SystemMetricsEvent {

  private ApplicationId appId;
  private String name;
  private String type;
  private String user;
  private String queue;
  private long submittedTime;
  private Set<String> appTags;
  private boolean unmanagedApplication;
  private Priority applicationPriority;
  private String appNodeLabelsExpression;
  private String amNodeLabelsExpression;
  private final CallerContext callerContext;


  public ApplicationCreatedEvent(ApplicationId appId,
      String name,
      String type,
      String user,
      String queue,
      long submittedTime,
      long createdTime,
      Set<String> appTags,
      boolean unmanagedApplication,
      Priority applicationPriority,
      String appNodeLabelsExpression,
      String amNodeLabelsExpression,
      CallerContext callerContext) {
    super(SystemMetricsEventType.APP_CREATED, createdTime);
    this.appId = appId;
    this.name = name;
    this.type = type;
    this.user = user;
    this.queue = queue;
    this.submittedTime = submittedTime;
    this.appTags = appTags;
    this.unmanagedApplication = unmanagedApplication;
    this.applicationPriority = applicationPriority;
    this.appNodeLabelsExpression = appNodeLabelsExpression;
    this.amNodeLabelsExpression = amNodeLabelsExpression;
    this.callerContext = callerContext;
  }

  @Override
  public int hashCode() {
    return appId.hashCode();
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public String getApplicationName() {
    return name;
  }

  public String getApplicationType() {
    return type;
  }

  public String getUser() {
    return user;
  }

  public String getQueue() {
    return queue;
  }

  public long getSubmittedTime() {
    return submittedTime;
  }

  public Set<String> getAppTags() {
    return appTags;
  }

  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }

  public Priority getApplicationPriority() {
    return applicationPriority;
  }

  public String getAppNodeLabelsExpression() {
    return appNodeLabelsExpression;
  }

  public String getAmNodeLabelsExpression() {
    return amNodeLabelsExpression;
  }
  
  public CallerContext getCallerContext() {
    return callerContext;
  }
}
