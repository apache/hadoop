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

package org.apache.hadoop.yarn.api;

import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

public interface ClientRMProtocol {
  public GetNewApplicationIdResponse getNewApplicationId(GetNewApplicationIdRequest request) throws YarnRemoteException;
  public GetApplicationReportResponse getApplicationReport(GetApplicationReportRequest request) throws YarnRemoteException;
  public SubmitApplicationResponse submitApplication(SubmitApplicationRequest request) throws YarnRemoteException;
  public FinishApplicationResponse finishApplication(FinishApplicationRequest request) throws YarnRemoteException;
  public GetClusterMetricsResponse getClusterMetrics(GetClusterMetricsRequest request) throws YarnRemoteException;
  public GetAllApplicationsResponse getAllApplications(GetAllApplicationsRequest request) throws YarnRemoteException;
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request) throws YarnRemoteException;
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request) throws YarnRemoteException;
  public GetQueueUserAclsInfoResponse getQueueUserAcls(GetQueueUserAclsInfoRequest request) throws YarnRemoteException;
}
