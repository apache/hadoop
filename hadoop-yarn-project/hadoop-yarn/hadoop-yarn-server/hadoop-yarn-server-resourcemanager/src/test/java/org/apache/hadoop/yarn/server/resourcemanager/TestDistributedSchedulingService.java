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
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.server.api.DistributedSchedulerProtocol;
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
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .AMLivelinessMonitor;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

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
    DistributedSchedulingService service =
        new DistributedSchedulingService(rmContext, null) {
          @Override
          public RegisterApplicationMasterResponse registerApplicationMaster
              (RegisterApplicationMasterRequest request) throws
              YarnException, IOException {
            RegisterApplicationMasterResponse resp = factory.newRecordInstance(
                RegisterApplicationMasterResponse.class);
            // Dummy Entry to Assert that we get this object back
            resp.setQueue("dummyQueue");
            return resp;
          }

          @Override
          public FinishApplicationMasterResponse finishApplicationMaster
              (FinishApplicationMasterRequest request) throws YarnException,
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
            AllocateResponse response = factory.newRecordInstance
                (AllocateResponse.class);
            response.setNumClusterNodes(12345);
            return response;
          }

          @Override
          public DistSchedRegisterResponse
          registerApplicationMasterForDistributedScheduling
              (RegisterApplicationMasterRequest request) throws
              YarnException, IOException {
            DistSchedRegisterResponse resp = factory.newRecordInstance(
                DistSchedRegisterResponse.class);
            resp.setContainerIdStart(54321l);
            return resp;
          }

          @Override
          public DistSchedAllocateResponse allocateForDistributedScheduling
              (AllocateRequest request) throws YarnException, IOException {
            DistSchedAllocateResponse resp =
                factory.newRecordInstance(DistSchedAllocateResponse.class);
            resp.setNodesForScheduling(
                Arrays.asList(NodeId.newInstance("h1", 1234)));
            return resp;
          }
        };
    Server server = service.getServer(rpc, conf, addr, null);
    server.start();

    // Verify that the DistrubutedSchedulingService can handle vanilla
    // ApplicationMasterProtocol clients
    RPC.setProtocolEngine(conf, ApplicationMasterProtocolPB.class,
        ProtobufRpcEngine.class);
    ApplicationMasterProtocol ampProxy =
        (ApplicationMasterProtocol) rpc.getProxy(ApplicationMasterProtocol
            .class, NetUtils.getConnectAddress(server), conf);
    RegisterApplicationMasterResponse regResp = ampProxy.registerApplicationMaster(
            factory.newRecordInstance(RegisterApplicationMasterRequest.class));
    Assert.assertEquals("dummyQueue", regResp.getQueue());
    FinishApplicationMasterResponse finishResp = ampProxy
        .finishApplicationMaster(factory.newRecordInstance(
            FinishApplicationMasterRequest.class));
    Assert.assertEquals(false, finishResp.getIsUnregistered());
    AllocateResponse allocResp = ampProxy
        .allocate(factory.newRecordInstance(AllocateRequest.class));
    Assert.assertEquals(12345, allocResp.getNumClusterNodes());


    // Verify that the DistrubutedSchedulingService can handle the
    // DistributedSchedulerProtocol clients as well
    RPC.setProtocolEngine(conf, DistributedSchedulerProtocolPB.class,
        ProtobufRpcEngine.class);
    DistributedSchedulerProtocol dsProxy =
        (DistributedSchedulerProtocol) rpc.getProxy(DistributedSchedulerProtocol
            .class, NetUtils.getConnectAddress(server), conf);

    DistSchedRegisterResponse dsRegResp =
        dsProxy.registerApplicationMasterForDistributedScheduling(
        factory.newRecordInstance(RegisterApplicationMasterRequest.class));
    Assert.assertEquals(54321l, dsRegResp.getContainerIdStart());
    DistSchedAllocateResponse dsAllocResp =
        dsProxy.allocateForDistributedScheduling(
            factory.newRecordInstance(AllocateRequest.class));
    Assert.assertEquals(
        "h1", dsAllocResp.getNodesForScheduling().get(0).getHost());
  }
}
