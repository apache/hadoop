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

package org.apache.hadoop.yarn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.junit.Assert;
import org.junit.Test;

/*
 * Test that the container launcher rpc times out properly. This is used
 * by both RM to launch an AM as well as an AM to launch containers.
 */
public class TestContainerLaunchRPC {

  static final Log LOG = LogFactory.getLog(TestContainerLaunchRPC.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @Test
  public void testHadoopProtoRPCTimeout() throws Exception {
    testRPCTimeout(HadoopYarnProtoRPC.class.getName());
  }

  private void testRPCTimeout(String rpcClass) throws Exception {
    Configuration conf = new Configuration();
    // set timeout low for the test
    conf.setInt("yarn.rpc.nm-command-timeout", 3000);

    conf.set(YarnConfiguration.IPC_RPC_IMPL, rpcClass);
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    Server server = rpc.getServer(ContainerManagementProtocol.class,
        new DummyContainerManager(), addr, conf, null, 1);
    server.start();
    try {

      ContainerManagementProtocol proxy = (ContainerManagementProtocol) rpc.getProxy(
          ContainerManagementProtocol.class,
          server.getListenerAddress(), conf);
      ContainerLaunchContext containerLaunchContext = recordFactory
          .newRecordInstance(ContainerLaunchContext.class);

      ApplicationId applicationId = ApplicationId.newInstance(0, 0);
      ApplicationAttemptId applicationAttemptId =
          ApplicationAttemptId.newInstance(applicationId, 0);
      ContainerId containerId =
          ContainerId.newContainerId(applicationAttemptId, 100);
      NodeId nodeId = NodeId.newInstance("localhost", 1234);
      Resource resource = Resource.newInstance(1234, 2);
      ContainerTokenIdentifier containerTokenIdentifier =
          new ContainerTokenIdentifier(containerId, "localhost", "user",
            resource, System.currentTimeMillis() + 10000, 42, 42,
            Priority.newInstance(0), 0);
      Token containerToken =
          TestRPC.newContainerToken(nodeId, "password".getBytes(),
            containerTokenIdentifier);

      StartContainerRequest scRequest =
          StartContainerRequest.newInstance(containerLaunchContext,
            containerToken);
      List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
      list.add(scRequest);
      StartContainersRequest allRequests =
          StartContainersRequest.newInstance(list);
      try {
        proxy.startContainers(allRequests);
      } catch (Exception e) {
        LOG.info(StringUtils.stringifyException(e));
        Assert.assertEquals("Error, exception is not: "
            + SocketTimeoutException.class.getName(),
            SocketTimeoutException.class.getName(), e.getClass().getName());
        return;
      }
    } finally {
      server.stop();
    }

    Assert.fail("timeout exception should have occurred!");
  }

  public class DummyContainerManager implements ContainerManagementProtocol {

    private ContainerStatus status = null;

    @Override
    public StartContainersResponse startContainers(
        StartContainersRequest requests) throws YarnException, IOException {
      try {
        // make the thread sleep to look like its not going to respond
        Thread.sleep(10000);
      } catch (Exception e) {
        LOG.error(e);
        throw new YarnException(e);
      }
      throw new YarnException("Shouldn't happen!!");
    }

    @Override
    public StopContainersResponse
        stopContainers(StopContainersRequest requests) throws YarnException,
            IOException {
      Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw new YarnException(e);
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
        GetContainerStatusesRequest request) throws YarnException, IOException {
      List<ContainerStatus> list = new ArrayList<ContainerStatus>();
      list.add(status);
      GetContainerStatusesResponse response =
          GetContainerStatusesResponse.newInstance(list, null);
      return null;
    }

    @Override
    public IncreaseContainersResourceResponse increaseContainersResource(
        IncreaseContainersResourceRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public SignalContainerResponse signalToContainer(
        SignalContainerRequest request) throws YarnException, IOException {
      final Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw new YarnException(e);
    }
  }
}
