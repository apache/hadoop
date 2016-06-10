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

package org.apache.hadoop.yarn.server.nodemanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedRegisterResponse;
import org.apache.hadoop.yarn.api.protocolrecords
    .RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords
    .RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.RequestInterceptor;
import org.apache.hadoop.yarn.server.nodemanager.security
    .NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security
    .NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestLocalScheduler {

  @Test
  public void testLocalScheduler() throws Exception {

    Configuration conf = new Configuration();
    LocalScheduler localScheduler = new LocalScheduler();

    NodeStatusUpdater nodeStatusUpdater = Mockito.mock(NodeStatusUpdater.class);
    Mockito.when(nodeStatusUpdater.getRMIdentifier()).thenReturn(12345l);
    Context context = Mockito.mock(Context.class);
    NMContainerTokenSecretManager nmContainerTokenSecretManager = new
        NMContainerTokenSecretManager(conf);
    MasterKey mKey = new MasterKey() {
      @Override
      public int getKeyId() {
        return 1;
      }
      @Override
      public void setKeyId(int keyId) {}
      @Override
      public ByteBuffer getBytes() {
        return ByteBuffer.allocate(8);
      }
      @Override
      public void setBytes(ByteBuffer bytes) {}
    };
    nmContainerTokenSecretManager.setMasterKey(mKey);
    Mockito.when(context.getContainerTokenSecretManager()).thenReturn
        (nmContainerTokenSecretManager);
    OpportunisticContainerAllocator containerAllocator =
        new OpportunisticContainerAllocator(nodeStatusUpdater, context, 7777);

    NMTokenSecretManagerInNM nmTokenSecretManagerInNM =
        new NMTokenSecretManagerInNM();
    nmTokenSecretManagerInNM.setMasterKey(mKey);
    localScheduler.initLocal(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1),
        containerAllocator, nmTokenSecretManagerInNM, "test");

    RequestInterceptor finalReqIntcptr = Mockito.mock(RequestInterceptor.class);
    localScheduler.setNextInterceptor(finalReqIntcptr);

    DistSchedRegisterResponse distSchedRegisterResponse =
        Records.newRecord(DistSchedRegisterResponse.class);
    distSchedRegisterResponse.setRegisterResponse(
        Records.newRecord(RegisterApplicationMasterResponse.class));
    distSchedRegisterResponse.setContainerTokenExpiryInterval(12345);
    distSchedRegisterResponse.setContainerIdStart(0);
    distSchedRegisterResponse.setMaxAllocatableCapabilty(
        Resource.newInstance(1024, 4));
    distSchedRegisterResponse.setMinAllocatableCapabilty(
        Resource.newInstance(512, 2));
    distSchedRegisterResponse.setNodesForScheduling(Arrays.asList(
        NodeId.newInstance("a", 1), NodeId.newInstance("b", 2)));
    Mockito.when(
        finalReqIntcptr.registerApplicationMasterForDistributedScheduling(
            Mockito.any(RegisterApplicationMasterRequest.class)))
        .thenReturn(distSchedRegisterResponse);

    localScheduler.registerApplicationMaster(
        Records.newRecord(RegisterApplicationMasterRequest.class));

    Mockito.when(
        finalReqIntcptr.allocateForDistributedScheduling(
            Mockito.any(AllocateRequest.class)))
        .thenAnswer(new Answer<DistSchedAllocateResponse>() {
          @Override
          public DistSchedAllocateResponse answer(InvocationOnMock
              invocationOnMock) throws Throwable {
            return createAllocateResponse(Arrays.asList(
                NodeId.newInstance("c", 3), NodeId.newInstance("d", 4)));
          }
        });

    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
    ResourceRequest guaranteedReq = Records.newRecord(ResourceRequest.class);
    guaranteedReq.setExecutionTypeRequest(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true));
    guaranteedReq.setNumContainers(5);
    guaranteedReq.setCapability(Resource.newInstance(2048, 2));
    guaranteedReq.setRelaxLocality(true);
    guaranteedReq.setResourceName("*");
    ResourceRequest opportunisticReq = Records.newRecord(ResourceRequest.class);
    opportunisticReq.setExecutionTypeRequest(
        ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC, true));
    opportunisticReq.setNumContainers(4);
    opportunisticReq.setCapability(Resource.newInstance(1024, 4));
    opportunisticReq.setPriority(Priority.newInstance(100));
    opportunisticReq.setRelaxLocality(true);
    opportunisticReq.setResourceName("*");
    allocateRequest.setAskList(Arrays.asList(guaranteedReq, opportunisticReq));

    // Verify 4 containers were allocated
    AllocateResponse allocateResponse = localScheduler.allocate(allocateRequest);
    Assert.assertEquals(4, allocateResponse.getAllocatedContainers().size());

    // Verify equal distribution on hosts a and b
    // And None on c and d
    Map<NodeId, List<ContainerId>> allocs = mapAllocs(allocateResponse);
    Assert.assertEquals(2, allocs.get(NodeId.newInstance("a", 1)).size());
    Assert.assertEquals(2, allocs.get(NodeId.newInstance("b", 2)).size());
    Assert.assertNull(allocs.get(NodeId.newInstance("c", 3)));
    Assert.assertNull(allocs.get(NodeId.newInstance("d", 4)));

    // New Allocate request
    allocateRequest = Records.newRecord(AllocateRequest.class);
    opportunisticReq = Records.newRecord(ResourceRequest.class);
    opportunisticReq.setExecutionTypeRequest(
        ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC, true));
    opportunisticReq.setNumContainers(6);
    opportunisticReq.setCapability(Resource.newInstance(512, 3));
    opportunisticReq.setPriority(Priority.newInstance(100));
    opportunisticReq.setRelaxLocality(true);
    opportunisticReq.setResourceName("*");
    allocateRequest.setAskList(Arrays.asList(guaranteedReq, opportunisticReq));

    // Verify 6 containers were allocated
    allocateResponse = localScheduler.allocate(allocateRequest);
    Assert.assertEquals(6, allocateResponse.getAllocatedContainers().size());

    // Verify New containers are equally distribution on hosts c and d
    // And None on a and b
    allocs = mapAllocs(allocateResponse);
    Assert.assertEquals(3, allocs.get(NodeId.newInstance("c", 3)).size());
    Assert.assertEquals(3, allocs.get(NodeId.newInstance("d", 4)).size());
    Assert.assertNull(allocs.get(NodeId.newInstance("a", 1)));
    Assert.assertNull(allocs.get(NodeId.newInstance("b", 2)));
  }

  private DistSchedAllocateResponse createAllocateResponse(List<NodeId> nodes) {
    DistSchedAllocateResponse distSchedAllocateResponse = Records.newRecord
        (DistSchedAllocateResponse.class);
    distSchedAllocateResponse.setAllocateResponse(
        Records.newRecord(AllocateResponse.class));
    distSchedAllocateResponse.setNodesForScheduling(nodes);
    return distSchedAllocateResponse;
  }

  private Map<NodeId, List<ContainerId>> mapAllocs(AllocateResponse
      allocateResponse) throws Exception {
    Map<NodeId, List<ContainerId>> allocs = new HashMap<>();
    for (Container c : allocateResponse.getAllocatedContainers()) {
      ContainerTokenIdentifier cTokId = BuilderUtils
          .newContainerTokenIdentifier(c.getContainerToken());
      Assert.assertEquals(
          c.getNodeId().getHost() + ":" + c.getNodeId().getPort(),
          cTokId.getNmHostAddress());
      List<ContainerId> cIds = allocs.get(c.getNodeId());
      if (cIds == null) {
        cIds = new ArrayList<>();
        allocs.put(c.getNodeId(), cIds);
      }
      cIds.add(c.getId());
    }
    return allocs;
  }

}
