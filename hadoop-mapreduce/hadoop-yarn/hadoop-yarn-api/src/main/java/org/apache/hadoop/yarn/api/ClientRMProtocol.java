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
