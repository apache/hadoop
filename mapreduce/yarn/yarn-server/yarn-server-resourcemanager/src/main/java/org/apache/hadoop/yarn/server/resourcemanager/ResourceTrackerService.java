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

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.RMNMSecurityInfoClass;
import org.apache.hadoop.yarn.server.YarnServerConfig;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.service.AbstractService;

public class ResourceTrackerService extends AbstractService 
implements ResourceTracker{

  private static final Log LOG = LogFactory.getLog(ResourceTrackerService.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private final RMResourceTrackerImpl resourceTracker;

  private Server server;
  private InetSocketAddress resourceTrackerAddress;

  public ResourceTrackerService(RMResourceTrackerImpl resourceTracker) {
    super(ResourceTrackerService.class.getName());
    this.resourceTracker = resourceTracker;
  }

  public RMResourceTrackerImpl getResourceTracker() {
    return resourceTracker;
  }

  @Override
  public synchronized void init(Configuration conf) {
    super.init(conf);
    String resourceTrackerBindAddress =
      conf.get(YarnServerConfig.RESOURCETRACKER_ADDRESS,
          YarnServerConfig.DEFAULT_RESOURCETRACKER_BIND_ADDRESS);
    resourceTrackerAddress = NetUtils.createSocketAddr(resourceTrackerBindAddress);
    resourceTracker.init(conf);
  }

  @Override
  public synchronized void start() {
    super.start();
    // ResourceTrackerServer authenticates NodeManager via Kerberos if
    // security is enabled, so no secretManager.
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration rtServerConf = new Configuration(getConfig());
    rtServerConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        RMNMSecurityInfoClass.class, SecurityInfo.class);
    this.server =
      rpc.getServer(ResourceTracker.class, this, resourceTrackerAddress,
          rtServerConf, null,
          rtServerConf.getInt(RMConfig.RM_RESOURCE_TRACKER_THREADS, 
              RMConfig.DEFAULT_RM_RESOURCE_TRACKER_THREADS));
    this.server.start();

    resourceTracker.start();
  }

  @Override
  public synchronized void stop() {
    resourceTracker.stop();
    if (this.server != null) {
      this.server.close();
    }
    super.stop();
  }

  @Override
  public RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnRemoteException {
    RegisterNodeManagerResponse response = recordFactory.newRecordInstance(
        RegisterNodeManagerResponse.class);
    try {
      response.setRegistrationResponse(
          resourceTracker.registerNodeManager(
              request.getHost(), request.getContainerManagerPort(), 
              request.getHttpPort(), request.getResource()));
    } catch (IOException ioe) {
      LOG.info("Exception in node registration from " + request.getHost(), ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
    return response;
  }

  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnRemoteException {
    NodeHeartbeatResponse response = recordFactory.newRecordInstance(
        NodeHeartbeatResponse.class);
    try {
    response.setHeartbeatResponse(
        resourceTracker.nodeHeartbeat(request.getNodeStatus()));
    } catch (IOException ioe) {
      LOG.info("Exception in heartbeat from node " + 
          request.getNodeStatus().getNodeId(), ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
    return response;
  }

  public void recover(RMState state) {
    resourceTracker.recover(state);
  }

}
