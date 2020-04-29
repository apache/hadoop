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

package org.apache.hadoop.yarn.client.api.impl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.AHSClient;
import org.apache.hadoop.yarn.client.api.TimelineReaderClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.timeline.TimelineEntityV2Converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides Application History client implementation which uses
 * ATS v2 as backend.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AHSv2ClientImpl extends AHSClient {
  private TimelineReaderClient readerClient;
  private String logServerUrl;

  public AHSv2ClientImpl() {
    super(AHSv2ClientImpl.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) {
    logServerUrl = conf.get(
        YarnConfiguration.YARN_LOG_SERVER_URL);
    readerClient = TimelineReaderClient.createTimelineReaderClient();
    readerClient.init(conf);
  }

  @VisibleForTesting
  protected void setReaderClient(TimelineReaderClient readerClient) {
    this.readerClient = readerClient;
  }

  @Override
  public void serviceStart() {
    readerClient.start();
  }

  @Override
  public void serviceStop() {
    readerClient.stop();
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    TimelineEntity entity = readerClient.getApplicationEntity(
        appId, "ALL", null);
    return TimelineEntityV2Converter.convertToApplicationReport(entity);
  }

  @Override
  public List<ApplicationReport> getApplications()
      throws YarnException, IOException {
    throw new UnsupportedOperationException("ATSv2.0 doesn't support retrieving"
        + " ALL application entities.");
  }

  @Override
  public ApplicationAttemptReport getApplicationAttemptReport(
      ApplicationAttemptId applicationAttemptId)
      throws YarnException, IOException {
    TimelineEntity entity = readerClient.getApplicationAttemptEntity(
        applicationAttemptId, "ALL", null);
    return TimelineEntityV2Converter.convertToApplicationAttemptReport(entity);
  }

  @Override
  public List<ApplicationAttemptReport> getApplicationAttempts(
      ApplicationId applicationId) throws YarnException, IOException {
    List<TimelineEntity> entities = readerClient.getApplicationAttemptEntities(
        applicationId, "ALL", null, 0, null);
    List<ApplicationAttemptReport> appAttemptReports =
        new ArrayList<>();
    if (entities != null && !entities.isEmpty()) {
      for (TimelineEntity entity : entities) {
        ApplicationAttemptReport container =
            TimelineEntityV2Converter.convertToApplicationAttemptReport(
                entity);
        appAttemptReports.add(container);
      }
    }
    return appAttemptReports;
  }

  @Override
  public ContainerReport getContainerReport(ContainerId containerId)
      throws YarnException, IOException {
    ApplicationReport appReport = getApplicationReport(
        containerId.getApplicationAttemptId().getApplicationId());
    TimelineEntity entity = readerClient.getContainerEntity(containerId,
        "ALL", null);
    return TimelineEntityV2Converter.convertToContainerReport(
        entity, logServerUrl, appReport.getUser());
  }

  @Override
  public List<ContainerReport> getContainers(ApplicationAttemptId
      applicationAttemptId) throws  YarnException, IOException {
    ApplicationId appId = applicationAttemptId.getApplicationId();
    ApplicationReport appReport = getApplicationReport(appId);
    Map<String, String> filters = new HashMap<>();
    filters.put("infofilters", "SYSTEM_INFO_PARENT_ENTITY eq {\"id\":\"" +
        applicationAttemptId.toString() +
        "\",\"type\":\"YARN_APPLICATION_ATTEMPT\"}");
    List<TimelineEntity> entities = readerClient.getContainerEntities(
        appId, "ALL", filters, 0, null);
    List<ContainerReport> containers =
        new ArrayList<>();
    if (entities != null && !entities.isEmpty()) {
      for (TimelineEntity entity : entities) {
        ContainerReport container =
            TimelineEntityV2Converter.convertToContainerReport(
            entity, logServerUrl, appReport.getUser());
        containers.add(container);
      }
    }
    return containers;
  }
}
