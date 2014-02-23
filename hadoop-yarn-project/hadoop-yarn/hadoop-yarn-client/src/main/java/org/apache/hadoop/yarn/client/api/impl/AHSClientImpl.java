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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.client.AHSProxy;
import org.apache.hadoop.yarn.client.api.AHSClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

@Private
@Unstable
public class AHSClientImpl extends AHSClient {

  protected ApplicationHistoryProtocol ahsClient;
  protected InetSocketAddress ahsAddress;

  public AHSClientImpl() {
    super(AHSClientImpl.class.getName());
  }

  private static InetSocketAddress getAHSAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_PORT);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.ahsAddress = getAHSAddress(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      ahsClient = AHSProxy.createAHSProxy(getConfig(),
          ApplicationHistoryProtocol.class, this.ahsAddress);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.ahsClient != null) {
      RPC.stopProxy(this.ahsClient);
    }
    super.serviceStop();
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    GetApplicationReportRequest request = GetApplicationReportRequest
        .newInstance(appId);
    GetApplicationReportResponse response = ahsClient
        .getApplicationReport(request);
    return response.getApplicationReport();
  }

  @Override
  public List<ApplicationReport> getApplications() throws YarnException,
      IOException {
    GetApplicationsRequest request = GetApplicationsRequest.newInstance(null,
        null);
    GetApplicationsResponse response = ahsClient.getApplications(request);
    return response.getApplicationList();
  }

  @Override
  public ApplicationAttemptReport getApplicationAttemptReport(
      ApplicationAttemptId applicationAttemptId) throws YarnException,
      IOException {
    GetApplicationAttemptReportRequest request = GetApplicationAttemptReportRequest
        .newInstance(applicationAttemptId);
    GetApplicationAttemptReportResponse response = ahsClient
        .getApplicationAttemptReport(request);
    return response.getApplicationAttemptReport();
  }

  @Override
  public List<ApplicationAttemptReport> getApplicationAttempts(
      ApplicationId appId) throws YarnException, IOException {
    GetApplicationAttemptsRequest request = GetApplicationAttemptsRequest
        .newInstance(appId);
    GetApplicationAttemptsResponse response = ahsClient
        .getApplicationAttempts(request);
    return response.getApplicationAttemptList();
  }

  @Override
  public ContainerReport getContainerReport(ContainerId containerId)
      throws YarnException, IOException {
    GetContainerReportRequest request = GetContainerReportRequest
        .newInstance(containerId);
    GetContainerReportResponse response = ahsClient.getContainerReport(request);
    return response.getContainerReport();
  }

  @Override
  public List<ContainerReport> getContainers(
      ApplicationAttemptId applicationAttemptId) throws YarnException,
      IOException {
    GetContainersRequest request = GetContainersRequest
        .newInstance(applicationAttemptId);
    GetContainersResponse response = ahsClient.getContainers(request);
    return response.getContainerList();
  }

}
