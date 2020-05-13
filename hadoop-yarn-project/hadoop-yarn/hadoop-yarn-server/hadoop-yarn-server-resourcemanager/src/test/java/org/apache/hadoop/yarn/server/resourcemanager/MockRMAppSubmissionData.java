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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class holds all data fields to submit an RM Application.
 * Use the {@link Builder} class to compile necessary data for app submission.
 */
public final class MockRMAppSubmissionData {
  private final List<ResourceRequest> amResourceRequests;
  private final String name;
  private final String user;
  private final Map<ApplicationAccessType, String> acls;
  private final boolean unmanaged;
  private final String queue;
  private final int maxAppAttempts;
  private final String appType;
  private final boolean waitForAccepted;
  private final boolean keepContainers;
  private final boolean isAppIdProvided;
  private final ApplicationId applicationId;
  private final long attemptFailuresValidityInterval;
  private final LogAggregationContext logAggregationContext;
  private final boolean cancelTokensWhenComplete;
  private final Priority priority;
  private final String amLabel;
  private final Map<ApplicationTimeoutType, Long> applicationTimeouts;
  private final ByteBuffer tokensConf;
  private final Set<String> applicationTags;
  private final String appNodeLabel;
  private final Credentials credentials;
  private final Resource resource;

  public List<ResourceRequest> getAmResourceRequests() {
    return amResourceRequests;
  }

  public String getName() {
    return name;
  }

  public String getUser() {
    return user;
  }

  public Map<ApplicationAccessType, String> getAcls() {
    return acls;
  }

  public boolean isUnmanaged() {
    return unmanaged;
  }

  public String getQueue() {
    return queue;
  }

  public int getMaxAppAttempts() {
    return maxAppAttempts;
  }

  public String getAppType() {
    return appType;
  }

  public boolean isWaitForAccepted() {
    return waitForAccepted;
  }

  public boolean isKeepContainers() {
    return keepContainers;
  }

  public boolean isAppIdProvided() {
    return isAppIdProvided;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public long getAttemptFailuresValidityInterval() {
    return attemptFailuresValidityInterval;
  }

  public LogAggregationContext getLogAggregationContext() {
    return logAggregationContext;
  }

  public boolean isCancelTokensWhenComplete() {
    return cancelTokensWhenComplete;
  }

  public Priority getPriority() {
    return priority;
  }

  public String getAmLabel() {
    return amLabel;
  }

  public Map<ApplicationTimeoutType, Long> getApplicationTimeouts() {
    return applicationTimeouts;
  }

  public ByteBuffer getTokensConf() {
    return tokensConf;
  }

  public Set<String> getApplicationTags() {
    return applicationTags;
  }

  public String getAppNodeLabel() {
    return appNodeLabel;
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public Resource getResource() {
    return resource;
  }

  private MockRMAppSubmissionData(Builder builder) {
    this.amLabel = builder.amLabel;
    this.tokensConf = builder.tokensConf;
    this.maxAppAttempts = builder.maxAppAttempts;
    this.logAggregationContext = builder.logAggregationContext;
    this.queue = builder.queue;
    this.amResourceRequests = builder.amResourceRequests;
    this.user = builder.user;
    this.priority = builder.priority;
    this.waitForAccepted = builder.waitForAccepted;
    this.keepContainers = builder.keepContainers;
    this.name = builder.name;
    this.applicationId = builder.applicationId;
    this.attemptFailuresValidityInterval =
        builder.attemptFailuresValidityInterval;
    this.acls = builder.acls;
    this.appType = builder.appType;
    this.appNodeLabel = builder.appNodeLabel;
    this.isAppIdProvided = builder.isAppIdProvided;
    this.unmanaged = builder.unmanaged;
    this.applicationTags = builder.applicationTags;
    this.cancelTokensWhenComplete = builder.cancelTokensWhenComplete;
    this.applicationTimeouts = builder.applicationTimeouts;
    this.credentials = builder.credentials;
    this.resource = builder.resource;
  }

  /**
   * Tests should use this class to prepare all data required to submit an app.
   */
  public static final class Builder {
    private List<ResourceRequest> amResourceRequests;
    private String name;
    private String user;
    private Map<ApplicationAccessType, String> acls;
    private boolean unmanaged;
    private String queue;
    private int maxAppAttempts;
    private String appType;
    private boolean waitForAccepted;
    private boolean keepContainers;
    private boolean isAppIdProvided;
    private ApplicationId applicationId;
    private long attemptFailuresValidityInterval;
    private LogAggregationContext logAggregationContext;
    private boolean cancelTokensWhenComplete;
    private Priority priority;
    private String amLabel;
    private Map<ApplicationTimeoutType, Long> applicationTimeouts;
    private ByteBuffer tokensConf;
    private Set<String> applicationTags;
    private String appNodeLabel;
    private Credentials credentials;
    private Resource resource;

    private Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    public static Builder createWithMemory(long memory, MockRM mockRM)
        throws IOException {
      Resource resource = Records.newRecord(Resource.class);
      resource.setMemorySize(memory);
      return createWithResource(resource, mockRM);
    }

    public static Builder createWithResource(Resource resource, MockRM mockRM)
        throws IOException {
      int maxAppAttempts =
          mockRM.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
              YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      return MockRMAppSubmissionData.Builder.create()
          .withResource(resource)
          .withAppName("")
          .withUser(UserGroupInformation
              .getCurrentUser().getShortUserName())
          .withAcls(null)
          .withUnmanagedAM(false)
          .withQueue(null)
          .withMaxAppAttempts(maxAppAttempts)
          .withCredentials(null)
          .withAppType(null)
          .withWaitForAppAcceptedState(true)
          .withKeepContainers(false)
          .withApplicationId(null)
          .withAttemptFailuresValidityInterval(0L)
          .withLogAggregationContext(null)
          .withCancelTokensWhenComplete(true)
          .withAppPriority(Priority.newInstance(0))
          .withAmLabel("")
          .withApplicationTimeouts(null)
          .withTokensConf(null);
    }

    public Builder withAmResourceRequests(
        List<ResourceRequest> amResourceRequests) {
      this.amResourceRequests = amResourceRequests;
      return this;
    }

    public Builder withAppName(String name) {
      this.name = name;
      return this;
    }

    public Builder withUser(String user) {
      this.user = user;
      return this;
    }

    public Builder withAcls(Map<ApplicationAccessType, String> acls) {
      this.acls = acls;
      return this;
    }

    public Builder withUnmanagedAM(boolean unmanaged) {
      this.unmanaged = unmanaged;
      return this;
    }

    public Builder withQueue(String queue) {
      this.queue = queue;
      return this;
    }

    public Builder withMaxAppAttempts(int maxAppAttempts) {
      this.maxAppAttempts = maxAppAttempts;
      return this;
    }

    public Builder withAppType(String appType) {
      this.appType = appType;
      return this;
    }

    public Builder withWaitForAppAcceptedState(boolean waitForAccepted) {
      this.waitForAccepted = waitForAccepted;
      return this;
    }

    public Builder withKeepContainers(boolean keepContainers) {
      this.keepContainers = keepContainers;
      return this;
    }

    public Builder withApplicationId(ApplicationId applicationId) {
      this.applicationId = applicationId;
      this.isAppIdProvided = applicationId != null;
      return this;
    }

    public Builder withAttemptFailuresValidityInterval(
        long attemptFailuresValidityInterval) {
      this.attemptFailuresValidityInterval = attemptFailuresValidityInterval;
      return this;
    }

    public Builder withLogAggregationContext(
        LogAggregationContext logAggregationContext) {
      this.logAggregationContext = logAggregationContext;
      return this;
    }

    public Builder withCancelTokensWhenComplete(
        boolean cancelTokensWhenComplete) {
      this.cancelTokensWhenComplete = cancelTokensWhenComplete;
      return this;
    }

    public Builder withAppPriority(Priority priority) {
      this.priority = priority;
      return this;
    }

    public Builder withAmLabel(String amLabel) {
      this.amLabel = amLabel;
      return this;
    }

    public Builder withApplicationTimeouts(
        Map<ApplicationTimeoutType, Long> applicationTimeouts) {
      this.applicationTimeouts = applicationTimeouts;
      return this;
    }

    public Builder withTokensConf(ByteBuffer tokensConf) {
      this.tokensConf = tokensConf;
      return this;
    }

    public Builder withApplicationTags(Set<String> applicationTags) {
      this.applicationTags = applicationTags;
      return this;
    }

    public Builder withAppNodeLabel(String appNodeLabel) {
      this.appNodeLabel = appNodeLabel;
      return this;
    }

    public Builder withCredentials(Credentials cred) {
      this.credentials = cred;
      return this;
    }

    public Builder withResource(Resource resource) {
      this.resource = resource;
      return this;
    }

    public MockRMAppSubmissionData build() {
      return new MockRMAppSubmissionData(this);
    }
  }
}
