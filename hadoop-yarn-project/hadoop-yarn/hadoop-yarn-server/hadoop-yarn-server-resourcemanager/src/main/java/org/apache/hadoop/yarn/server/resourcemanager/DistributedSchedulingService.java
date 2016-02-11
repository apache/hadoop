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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.server.api.DistributedSchedulerProtocol;
import org.apache.hadoop.yarn.api.impl.pb.service.ApplicationMasterProtocolPBServiceImpl;


import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedRegisterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.ApplicationMasterProtocol.ApplicationMasterProtocolService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security
    .AMRMTokenSecretManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

public class DistributedSchedulingService extends ApplicationMasterService
    implements DistributedSchedulerProtocol {

  public DistributedSchedulingService(RMContext rmContext,
      YarnScheduler scheduler) {
    super(DistributedSchedulingService.class.getName(), rmContext, scheduler);
  }

  @Override
  public Server getServer(YarnRPC rpc, Configuration serverConf,
      InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
    Server server = rpc.getServer(DistributedSchedulerProtocol.class, this,
        addr, serverConf, secretManager,
        serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
    // To support application running no NMs that DO NOT support
    // Dist Scheduling...
    ((RPC.Server) server).addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        ApplicationMasterProtocolPB.class,
        ApplicationMasterProtocolService.newReflectiveBlockingService(
            new ApplicationMasterProtocolPBServiceImpl(this)));
    return server;
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    return super.registerApplicationMaster(request);
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster
      (FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    return super.finishApplicationMaster(request);
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request) throws
      YarnException, IOException {
    return super.allocate(request);
  }

  @Override
  public DistSchedRegisterResponse
  registerApplicationMasterForDistributedScheduling(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    RegisterApplicationMasterResponse response =
        registerApplicationMaster(request);
    DistSchedRegisterResponse dsResp = recordFactory
        .newRecordInstance(DistSchedRegisterResponse.class);
    dsResp.setRegisterResponse(response);
    dsResp.setMinAllocatableCapabilty(
        Resource.newInstance(
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_MIN_MEMORY,
                YarnConfiguration.DIST_SCHEDULING_MIN_MEMORY_DEFAULT),
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_MIN_VCORES,
                YarnConfiguration.DIST_SCHEDULING_MIN_VCORES_DEFAULT)
        )
    );
    dsResp.setMaxAllocatableCapabilty(
        Resource.newInstance(
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_MAX_MEMORY,
                YarnConfiguration.DIST_SCHEDULING_MAX_MEMORY_DEFAULT),
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_MAX_VCORES,
                YarnConfiguration.DIST_SCHEDULING_MAX_VCORES_DEFAULT)
        )
    );
    dsResp.setIncrAllocatableCapabilty(
        Resource.newInstance(
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_INCR_MEMORY,
                YarnConfiguration.DIST_SCHEDULING_INCR_MEMORY_DEFAULT),
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_INCR_VCORES,
                YarnConfiguration.DIST_SCHEDULING_INCR_VCORES_DEFAULT)
        )
    );
    dsResp.setContainerTokenExpiryInterval(
        getConfig().getInt(
            YarnConfiguration.DIST_SCHEDULING_CONTAINER_TOKEN_EXPIRY_MS,
            YarnConfiguration.
                DIST_SCHEDULING_CONTAINER_TOKEN_EXPIRY_MS_DEFAULT));
    dsResp.setContainerIdStart(
        this.rmContext.getEpoch() << ResourceManager.EPOCH_BIT_SHIFT);

    // Set nodes to be used for scheduling
    // TODO: The actual computation of the list will happen in YARN-4412
    // TODO: Till then, send the complete list
    dsResp.setNodesForScheduling(
        new ArrayList<>(this.rmContext.getRMNodes().keySet()));
    return dsResp;
  }

  @Override
  public DistSchedAllocateResponse allocateForDistributedScheduling
      (AllocateRequest request) throws YarnException, IOException {
    AllocateResponse response = allocate(request);
    DistSchedAllocateResponse dsResp = recordFactory.newRecordInstance
        (DistSchedAllocateResponse.class);
    dsResp.setAllocateResponse(response);
    dsResp.setNodesForScheduling(
        new ArrayList<>(this.rmContext.getRMNodes().keySet()));
    return dsResp;
  }
}
