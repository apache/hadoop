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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationMasterResponse;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.NodeManagerInfo;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.ClusterTracker;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.service.AbstractService;


/**
 * The client interface to the Resource Manager. This module handles all the rpc
 * interfaces to the resource manager from the client.
 */
public class ClientRMService extends AbstractService implements ClientRMProtocol {
  private static final Log LOG = LogFactory.getLog(ClientRMService.class);
  
  final private ClusterTracker clusterInfo;
  final private ApplicationsManager applicationsManager;
  final private ResourceScheduler scheduler;
  
  private String clientServiceBindAddress;
  private Server server;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  InetSocketAddress clientBindAddress;
  
  public ClientRMService(ApplicationsManager applicationsManager, 
        ClusterTracker clusterInfo, ResourceScheduler scheduler) {
    super(ClientRMService.class.getName());
    this.clusterInfo = clusterInfo;
    this.applicationsManager = applicationsManager;
    this.scheduler = scheduler;
  }
  
  @Override
  public void init(Configuration conf) {
    clientServiceBindAddress =
      conf.get(YarnConfiguration.APPSMANAGER_ADDRESS,
          YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS);
    clientBindAddress =
      NetUtils.createSocketAddr(clientServiceBindAddress);
    super.init(conf);
  }
  
  @Override
  public void start() {
    // All the clients to appsManager are supposed to be authenticated via
    // Kerberos if security is enabled, so no secretManager.
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration clientServerConf = new Configuration(getConfig());
    clientServerConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        ClientRMSecurityInfo.class, SecurityInfo.class);
    this.server =   
      rpc.getServer(ClientRMProtocol.class, this,
            clientBindAddress,
            clientServerConf, null,
            clientServerConf.getInt(RMConfig.RM_CLIENT_THREADS, 
                RMConfig.DEFAULT_RM_CLIENT_THREADS));
    this.server.start();
    super.start();
  }

  @Override
  public GetNewApplicationIdResponse getNewApplicationId(GetNewApplicationIdRequest request) throws YarnRemoteException {
    GetNewApplicationIdResponse response = recordFactory.newRecordInstance(GetNewApplicationIdResponse.class);
    response.setApplicationId(applicationsManager.getNewApplicationID());
    return response;
  }
  
  @Override
  public GetApplicationMasterResponse getApplicationMaster(GetApplicationMasterRequest request) throws YarnRemoteException {
    ApplicationId applicationId = request.getApplicationId();
    GetApplicationMasterResponse response = recordFactory.newRecordInstance(GetApplicationMasterResponse.class);
    response.setApplicationMaster(applicationsManager.getApplicationMaster(applicationId));
    return response;
  }

  public SubmitApplicationResponse submitApplication(SubmitApplicationRequest request) throws YarnRemoteException {
    ApplicationSubmissionContext context = request.getApplicationSubmissionContext();
    try {
      applicationsManager.submitApplication(context);
    } catch (IOException ie) {
      LOG.info("Exception in submitting application", ie);
      throw RPCUtil.getRemoteException(ie);
    }
    SubmitApplicationResponse response = recordFactory.newRecordInstance(SubmitApplicationResponse.class);
    return response;
  }

  @Override
  public FinishApplicationResponse finishApplication(FinishApplicationRequest request) throws YarnRemoteException {
    ApplicationId applicationId = request.getApplicationId();
    try {
      UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
      applicationsManager.finishApplication(applicationId, callerUGI);
    } catch(IOException ie) {
      LOG.info("Error finishing application ", ie);
    }
    FinishApplicationResponse response = recordFactory.newRecordInstance(FinishApplicationResponse.class);
    return response;
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(GetClusterMetricsRequest request) throws YarnRemoteException {
    GetClusterMetricsResponse response = recordFactory.newRecordInstance(GetClusterMetricsResponse.class);
    response.setClusterMetrics(clusterInfo.getClusterMetrics());
    return response;
  }
  
  @Override
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) throws YarnRemoteException {
    GetAllApplicationsResponse response = 
      recordFactory.newRecordInstance(GetAllApplicationsResponse.class);
    response.setApplicationList(applicationsManager.getApplications());
    return response;
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnRemoteException {
    GetClusterNodesResponse response = 
      recordFactory.newRecordInstance(GetClusterNodesResponse.class);
    List<NodeInfo> nodeInfos = clusterInfo.getAllNodeInfo();
    List<NodeManagerInfo> nodes = 
      new ArrayList<NodeManagerInfo>(nodeInfos.size());
    for (NodeInfo nodeInfo : nodeInfos) {
      nodes.add(createNodeManagerInfo(nodeInfo));
    }
    response.setNodeManagerList(nodes);
    return response;
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnRemoteException {
    GetQueueInfoResponse response =
      recordFactory.newRecordInstance(GetQueueInfoResponse.class);
    try {
      QueueInfo queueInfo = 
        scheduler.getQueueInfo(request.getQueueName(), 
            request.getIncludeApplications(), 
            request.getIncludeChildQueues(), 
            request.getRecursive());
      response.setQueueInfo(queueInfo);
    } catch (IOException ioe) {
      LOG.info("Failed to getQueueInfo for " + request.getQueueName(), ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
    
    return response;
  }

  private NodeManagerInfo createNodeManagerInfo(NodeInfo nodeInfo) {
    NodeManagerInfo node = 
      recordFactory.newRecordInstance(NodeManagerInfo.class);
    node.setNodeAddress(nodeInfo.getNodeAddress());
    node.setRackName(nodeInfo.getRackName());
    node.setCapability(nodeInfo.getTotalCapability());
    node.setUsed(nodeInfo.getUsedResource());
    node.setNumContainers(nodeInfo.getNumContainers());
    return node;
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
    GetQueueUserAclsInfoResponse response = 
      recordFactory.newRecordInstance(GetQueueUserAclsInfoResponse.class);
    response.setUserAclsInfoList(scheduler.getQueueUserAclInfo());
    return response;
  }

  @Override
  public void stop() {
    if (this.server != null) {
        this.server.close();
    }
    super.stop();
  }
}
