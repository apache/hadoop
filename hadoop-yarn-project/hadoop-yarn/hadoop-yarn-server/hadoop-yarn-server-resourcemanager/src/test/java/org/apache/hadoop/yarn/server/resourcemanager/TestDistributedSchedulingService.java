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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .FinishApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.api.DistributedSchedulerProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedRegisterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb
    .DistSchedAllocateResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb
    .DistSchedRegisterResponsePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .AMLivelinessMonitor;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

public class TestDistributedSchedulingService {

  // Test if the DistributedSchedulingService can handle both DSProtocol as
  // well as AMProtocol clients
  @Test
  public void testRPCWrapping() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.IPC_RPC_IMPL, HadoopYarnProtoRPC.class
        .getName());
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    conf.setSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS, addr);
    final RecordFactory factory = RecordFactoryProvider.getRecordFactory(null);
    final RMContext rmContext = new RMContextImpl() {
      @Override
      public AMLivelinessMonitor getAMLivelinessMonitor() {
        return null;
      }

      @Override
      public Configuration getYarnConfiguration() {
        return new YarnConfiguration();
      }
    };
    Container c = factory.newRecordInstance(Container.class);
    c.setExecutionType(ExecutionType.OPPORTUNISTIC);
    c.setId(
        ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(
                ApplicationId.newInstance(12345, 1), 2), 3));
    AllocateRequest allReq =
        (AllocateRequestPBImpl)factory.newRecordInstance(AllocateRequest.class);
    allReq.setAskList(Arrays.asList(
        ResourceRequest.newInstance(Priority.UNDEFINED, "a",
            Resource.newInstance(1, 2), 1, true, "exp",
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))));
    DistributedSchedulingService service = createService(factory, rmContext, c);
    Server server = service.getServer(rpc, conf, addr, null);
    server.start();

    // Verify that the DistrubutedSchedulingService can handle vanilla
    // ApplicationMasterProtocol clients
    RPC.setProtocolEngine(conf, ApplicationMasterProtocolPB.class,
        ProtobufRpcEngine.class);
    ApplicationMasterProtocolPB ampProxy =
        RPC.getProxy(ApplicationMasterProtocolPB
        .class, 1, NetUtils.getConnectAddress(server), conf);
    RegisterApplicationMasterResponse regResp =
        new RegisterApplicationMasterResponsePBImpl(
            ampProxy.registerApplicationMaster(null,
                ((RegisterApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(
                        RegisterApplicationMasterRequest.class)).getProto()));
    Assert.assertEquals("dummyQueue", regResp.getQueue());
    FinishApplicationMasterResponse finishResp =
        new FinishApplicationMasterResponsePBImpl(
            ampProxy.finishApplicationMaster(null,
                ((FinishApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(
                        FinishApplicationMasterRequest.class)).getProto()
            ));
    Assert.assertEquals(false, finishResp.getIsUnregistered());
    AllocateResponse allocResp =
        new AllocateResponsePBImpl(
            ampProxy.allocate(null,
                ((AllocateRequestPBImpl)factory
                    .newRecordInstance(AllocateRequest.class)).getProto())
        );
    List<Container> allocatedContainers = allocResp.getAllocatedContainers();
    Assert.assertEquals(1, allocatedContainers.size());
    Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
        allocatedContainers.get(0).getExecutionType());
    Assert.assertEquals(12345, allocResp.getNumClusterNodes());


    // Verify that the DistrubutedSchedulingService can handle the
    // DistributedSchedulerProtocol clients as well
    RPC.setProtocolEngine(conf, DistributedSchedulerProtocolPB.class,
        ProtobufRpcEngine.class);
    DistributedSchedulerProtocolPB dsProxy =
        RPC.getProxy(DistributedSchedulerProtocolPB
            .class, 1, NetUtils.getConnectAddress(server), conf);

    DistSchedRegisterResponse dsRegResp =
        new DistSchedRegisterResponsePBImpl(
            dsProxy.registerApplicationMasterForDistributedScheduling(null,
                ((RegisterApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(RegisterApplicationMasterRequest.class))
                    .getProto()));
    Assert.assertEquals(54321l, dsRegResp.getContainerIdStart());
    Assert.assertEquals(4,
        dsRegResp.getMaxAllocatableCapabilty().getVirtualCores());
    Assert.assertEquals(1024,
        dsRegResp.getMinAllocatableCapabilty().getMemorySize());
    Assert.assertEquals(2,
        dsRegResp.getIncrAllocatableCapabilty().getVirtualCores());

    DistSchedAllocateResponse dsAllocResp =
        new DistSchedAllocateResponsePBImpl(
            dsProxy.allocateForDistributedScheduling(null,
                ((AllocateRequestPBImpl)allReq).getProto()));
    Assert.assertEquals(
        "h1", dsAllocResp.getNodesForScheduling().get(0).getHost());

    FinishApplicationMasterResponse dsfinishResp =
        new FinishApplicationMasterResponsePBImpl(
            dsProxy.finishApplicationMaster(null,
                ((FinishApplicationMasterRequestPBImpl) factory
                    .newRecordInstance(FinishApplicationMasterRequest.class))
                    .getProto()));
    Assert.assertEquals(
        false, dsfinishResp.getIsUnregistered());
  }

  private DistributedSchedulingService createService(final RecordFactory
      factory, final RMContext rmContext, final Container c) {
    return new DistributedSchedulingService(rmContext, null) {
      @Override
      public RegisterApplicationMasterResponse registerApplicationMaster(
          RegisterApplicationMasterRequest request) throws
          YarnException, IOException {
        RegisterApplicationMasterResponse resp = factory.newRecordInstance(
            RegisterApplicationMasterResponse.class);
        // Dummy Entry to Assert that we get this object back
        resp.setQueue("dummyQueue");
        return resp;
      }

      @Override
      public FinishApplicationMasterResponse finishApplicationMaster(
          FinishApplicationMasterRequest request) throws YarnException,
          IOException {
        FinishApplicationMasterResponse resp = factory.newRecordInstance(
            FinishApplicationMasterResponse.class);
        // Dummy Entry to Assert that we get this object back
        resp.setIsUnregistered(false);
        return resp;
      }

      @Override
      public AllocateResponse allocate(AllocateRequest request) throws
          YarnException, IOException {
        AllocateResponse response = factory.newRecordInstance(
            AllocateResponse.class);
        response.setNumClusterNodes(12345);
        response.setAllocatedContainers(Arrays.asList(c));
        return response;
      }

      @Override
      public DistSchedRegisterResponse
          registerApplicationMasterForDistributedScheduling(
          RegisterApplicationMasterRequest request) throws
          YarnException, IOException {
        DistSchedRegisterResponse resp = factory.newRecordInstance(
            DistSchedRegisterResponse.class);
        resp.setContainerIdStart(54321L);
        resp.setMaxAllocatableCapabilty(Resource.newInstance(4096, 4));
        resp.setMinAllocatableCapabilty(Resource.newInstance(1024, 1));
        resp.setIncrAllocatableCapabilty(Resource.newInstance(2048, 2));
        return resp;
      }

      @Override
      public DistSchedAllocateResponse allocateForDistributedScheduling(
          AllocateRequest request) throws YarnException, IOException {
        List<ResourceRequest> askList = request.getAskList();
        Assert.assertEquals(1, askList.size());
        Assert.assertTrue(askList.get(0)
            .getExecutionTypeRequest().getEnforceExecutionType());
        DistSchedAllocateResponse resp =
            factory.newRecordInstance(DistSchedAllocateResponse.class);
        resp.setNodesForScheduling(
            Arrays.asList(NodeId.newInstance("h1", 1234)));
        return resp;
      }
    };
  }
}
