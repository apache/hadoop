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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
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
          ContainerId.newInstance(applicationAttemptId, 100);
      NodeId nodeId = NodeId.newInstance("localhost", 1234);
      Resource resource = Resource.newInstance(1234, 2);
      ContainerTokenIdentifier containerTokenIdentifier =
          new ContainerTokenIdentifier(containerId, "localhost", "user",
            resource, System.currentTimeMillis() + 10000, 42, 42);
      Token containerToken =
          TestRPC.newContainerToken(nodeId, "password".getBytes(),
            containerTokenIdentifier);

      StartContainerRequest scRequest = recordFactory
          .newRecordInstance(StartContainerRequest.class);
      scRequest.setContainerLaunchContext(containerLaunchContext);
      scRequest.setContainerToken(containerToken);
      try {
        proxy.startContainer(scRequest);
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
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request) throws YarnException {
      GetContainerStatusResponse response = recordFactory
          .newRecordInstance(GetContainerStatusResponse.class);
      response.setStatus(status);
      return response;
    }

    @Override
    public StartContainerResponse startContainer(StartContainerRequest request)
        throws YarnException, IOException {
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
    public StopContainerResponse stopContainer(StopContainerRequest request)
        throws YarnException {
      Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw new YarnException(e);
    }
  }
}
