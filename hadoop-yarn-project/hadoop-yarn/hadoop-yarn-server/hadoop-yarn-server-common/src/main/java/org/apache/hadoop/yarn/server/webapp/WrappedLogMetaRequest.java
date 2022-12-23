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
package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * WrappedLogMetaRequest is wrapping a log request initiated by the client.
 * This wrapper class translates the request to a {@link ContainerLogsRequest}
 * and calls #readAggregatedLogsMeta on the
 * {@link LogAggregationFileController}.
 * class.
 */
public class WrappedLogMetaRequest {

  private final LogAggregationFileControllerFactory factory;
  private final ApplicationId appId;
  private final String appOwner;
  private final ContainerId containerId;
  private final String nodeId;
  private final ApplicationAttemptId applicationAttemptId;

  private WrappedLogMetaRequest(Builder builder) {
    this.factory = builder.factory;
    this.appId = builder.appId;
    this.appOwner = builder.appOwner;
    this.containerId = builder.containerId;
    this.nodeId = builder.nodeId;
    this.applicationAttemptId = builder.applicationAttemptId;
  }

  public static class Builder {
    private LogAggregationFileControllerFactory factory;
    private ApplicationId appId;
    private String appOwner;
    private ContainerId containerId;
    private String nodeId;
    private ApplicationAttemptId applicationAttemptId;

    Builder() {
    }

    Builder setFactory(LogAggregationFileControllerFactory logFactory) {
      this.factory = logFactory;
      return this;
    }

    public Builder setApplicationId(ApplicationId applicationId) {
      this.appId = applicationId;
      return this;
    }

    Builder setNodeId(String nid) {
      this.nodeId = nid;
      return this;
    }

    public Builder setContainerId(@Nullable String containerIdStr) {
      if (containerIdStr != null) {
        this.containerId = ContainerId.fromString(containerIdStr);
      }
      return this;
    }

    Builder setAppOwner(String user) {
      this.appOwner = user;
      return this;
    }

    public Builder setApplicationAttemptId(ApplicationAttemptId appAttemptId) {
      this.applicationAttemptId = appAttemptId;
      return this;
    }

    String getAppId() {
      return WrappedLogMetaRequest.getAppId(appId, applicationAttemptId,
          containerId);
    }

    WrappedLogMetaRequest build() {
      if (this.factory == null) {
        throw new AssertionError("WrappedLogMetaRequest's builder should be " +
            "given a LogAggregationFileControllerFactory as parameter.");
      }
      return new WrappedLogMetaRequest(this);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private static String getAppId(ApplicationId appId,
      ApplicationAttemptId applicationAttemptId, ContainerId containerId) {
    if (appId == null) {
      if (applicationAttemptId == null) {
        return containerId.getApplicationAttemptId().getApplicationId()
            .toString();
      } else {
        return applicationAttemptId.getApplicationId().toString();
      }
    }
    return appId.toString();
  }

  public String getAppId() {
    return getAppId(appId, applicationAttemptId, containerId);
  }

  public String getAppAttemptId() {
    if (applicationAttemptId == null) {
      if (containerId != null) {
        return containerId.getApplicationAttemptId().toString();
      } else {
        return null;
      }
    } else {
      return applicationAttemptId.toString();
    }
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * Constructs a {@link ContainerLogsRequest} object, and obtains
   * {@link ContainerLogsRequest} from the corresponding
   * {@link LogAggregationFileController}.
   *
   * @return list of {@link ContainerLogMeta} objects that belong
   *         to the application, attempt or container
   */
  public List<ContainerLogMeta> getContainerLogMetas() throws IOException {
    ApplicationId applicationId = ApplicationId.fromString(getAppId());

    ContainerLogsRequest request = new ContainerLogsRequest();
    request.setAppId(applicationId);
    request.setAppAttemptId(applicationAttemptId);
    if (containerId != null) {
      request.setContainerId(containerId.toString());
    }
    request.setAppOwner(appOwner);
    request.setNodeId(nodeId);

    return factory.getFileControllerForRead(applicationId, appOwner)
        .readAggregatedLogsMeta(request);
  }
}
