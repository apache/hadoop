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
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedAllocateRequest;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class TestLocalScheduler {

  @Test
  public void testLocalScheduler() throws Exception {

    Configuration conf = new Configuration();
    LocalScheduler localScheduler = new LocalScheduler();

    RequestInterceptor finalReqIntcptr = setup(conf, localScheduler);

    registerAM(localScheduler, finalReqIntcptr, Arrays.asList(
        NodeId.newInstance("a", 1), NodeId.newInstance("b", 2)));

    final AtomicBoolean flipFlag = new AtomicBoolean(false);
    Mockito.when(
        finalReqIntcptr.allocateForDistributedScheduling(
            Mockito.any(DistSchedAllocateRequest.class)))
        .thenAnswer(new Answer<DistSchedAllocateResponse>() {
          @Override
          public DistSchedAllocateResponse answer(InvocationOnMock
              invocationOnMock) throws Throwable {
            flipFlag.set(!flipFlag.get());
            if (flipFlag.get()) {
              return createAllocateResponse(Arrays.asList(
                  NodeId.newInstance("c", 3), NodeId.newInstance("d", 4)));
            } else {
              return createAllocateResponse(Arrays.asList(
                  NodeId.newInstance("d", 4), NodeId.newInstance("c", 3)));
            }
          }
        });

    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
    ResourceRequest guaranteedReq =
        createResourceRequest(ExecutionType.GUARANTEED, 5, "*");

    ResourceRequest opportunisticReq =
        createResourceRequest(ExecutionType.OPPORTUNISTIC, 4, "*");
    allocateRequest.setAskList(Arrays.asList(guaranteedReq, opportunisticReq));

    // Verify 4 containers were allocated
    AllocateResponse allocateResponse =
        localScheduler.allocate(allocateRequest);
    Assert.assertEquals(4, allocateResponse.getAllocatedContainers().size());

    // Verify equal distribution on hosts a and b
    // And None on c and d
    Map<NodeId, List<ContainerId>> allocs = mapAllocs(allocateResponse, 4);
    Assert.assertEquals(2, allocs.get(NodeId.newInstance("a", 1)).size());
    Assert.assertEquals(2, allocs.get(NodeId.newInstance("b", 2)).size());
    Assert.assertNull(allocs.get(NodeId.newInstance("c", 3)));
    Assert.assertNull(allocs.get(NodeId.newInstance("d", 4)));

    // New Allocate request
    allocateRequest = Records.newRecord(AllocateRequest.class);
    opportunisticReq =
        createResourceRequest(ExecutionType.OPPORTUNISTIC, 6, "*");
    allocateRequest.setAskList(Arrays.asList(guaranteedReq, opportunisticReq));

    // Verify 6 containers were allocated
    allocateResponse = localScheduler.allocate(allocateRequest);
    Assert.assertEquals(6, allocateResponse.getAllocatedContainers().size());

    // Verify New containers are equally distribution on hosts c and d
    // And None on a and b
    allocs = mapAllocs(allocateResponse, 6);
    Assert.assertEquals(3, allocs.get(NodeId.newInstance("c", 3)).size());
    Assert.assertEquals(3, allocs.get(NodeId.newInstance("d", 4)).size());
    Assert.assertNull(allocs.get(NodeId.newInstance("a", 1)));
    Assert.assertNull(allocs.get(NodeId.newInstance("b", 2)));

    // Ensure the LocalScheduler respects the list order..
    // The first request should be allocated to "d" since it is ranked higher
    // The second request should be allocated to "c" since the ranking is
    // flipped on every allocate response.
    allocateRequest = Records.newRecord(AllocateRequest.class);
    opportunisticReq =
        createResourceRequest(ExecutionType.OPPORTUNISTIC, 1, "*");
    allocateRequest.setAskList(Arrays.asList(guaranteedReq, opportunisticReq));
    allocateResponse = localScheduler.allocate(allocateRequest);
    allocs = mapAllocs(allocateResponse, 1);
    Assert.assertEquals(1, allocs.get(NodeId.newInstance("d", 4)).size());

    allocateRequest = Records.newRecord(AllocateRequest.class);
    opportunisticReq =
        createResourceRequest(ExecutionType.OPPORTUNISTIC, 1, "*");
    allocateRequest.setAskList(Arrays.asList(guaranteedReq, opportunisticReq));
    allocateResponse = localScheduler.allocate(allocateRequest);
    allocs = mapAllocs(allocateResponse, 1);
    Assert.assertEquals(1, allocs.get(NodeId.newInstance("c", 3)).size());

    allocateRequest = Records.newRecord(AllocateRequest.class);
    opportunisticReq =
        createResourceRequest(ExecutionType.OPPORTUNISTIC, 1, "*");
    allocateRequest.setAskList(Arrays.asList(guaranteedReq, opportunisticReq));
    allocateResponse = localScheduler.allocate(allocateRequest);
    allocs = mapAllocs(allocateResponse, 1);
    Assert.assertEquals(1, allocs.get(NodeId.newInstance("d", 4)).size());
  }

  private void registerAM(LocalScheduler localScheduler, RequestInterceptor
      finalReqIntcptr, List<NodeId> nodeList) throws Exception {
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
    distSchedRegisterResponse.setNodesForScheduling(nodeList);
    Mockito.when(
        finalReqIntcptr.registerApplicationMasterForDistributedScheduling(
            Mockito.any(RegisterApplicationMasterRequest.class)))
        .thenReturn(distSchedRegisterResponse);

    localScheduler.registerApplicationMaster(
        Records.newRecord(RegisterApplicationMasterRequest.class));
  }

  private RequestInterceptor setup(Configuration conf, LocalScheduler
      localScheduler) {
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
    return finalReqIntcptr;
  }

  private ResourceRequest createResourceRequest(ExecutionType execType,
      int numContainers, String resourceName) {
    ResourceRequest opportunisticReq = Records.newRecord(ResourceRequest.class);
    opportunisticReq.setExecutionTypeRequest(
        ExecutionTypeRequest.newInstance(execType, true));
    opportunisticReq.setNumContainers(numContainers);
    opportunisticReq.setCapability(Resource.newInstance(1024, 4));
    opportunisticReq.setPriority(Priority.newInstance(100));
    opportunisticReq.setRelaxLocality(true);
    opportunisticReq.setResourceName(resourceName);
    return opportunisticReq;
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
      allocateResponse, int expectedSize) throws Exception {
    Assert.assertEquals(expectedSize,
        allocateResponse.getAllocatedContainers().size());
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
