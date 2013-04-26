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

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.factory.providers.YarnRemoteExceptionFactoryProvider;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

/*
 * Test that the container launcher rpc times out properly. This is used
 * by both RM to launch an AM as well as an AM to launch containers.
 */
public class TestContainerLaunchRPC {

  static final Log LOG = LogFactory.getLog(TestContainerLaunchRPC.class);

  private static final String EXCEPTION_CAUSE = "java.net.SocketTimeoutException";
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
    Server server = rpc.getServer(ContainerManager.class,
        new DummyContainerManager(), addr, conf, null, 1);
    server.start();
    try {

      ContainerManager proxy = (ContainerManager) rpc.getProxy(
          ContainerManager.class,
          server.getListenerAddress(), conf);
      ContainerLaunchContext containerLaunchContext = recordFactory
          .newRecordInstance(ContainerLaunchContext.class);
      containerLaunchContext.setUser("dummy-user");
      ContainerId containerId = recordFactory
          .newRecordInstance(ContainerId.class);
      ApplicationId applicationId = recordFactory
          .newRecordInstance(ApplicationId.class);
      ApplicationAttemptId applicationAttemptId = recordFactory
          .newRecordInstance(ApplicationAttemptId.class);
      applicationId.setClusterTimestamp(0);
      applicationId.setId(0);
      applicationAttemptId.setApplicationId(applicationId);
      applicationAttemptId.setAttemptId(0);
      containerId.setApplicationAttemptId(applicationAttemptId);
      containerId.setId(100);
      Container container =
          BuilderUtils.newContainer(containerId, null, null, recordFactory
              .newRecordInstance(Resource.class), null, null, 0);

      StartContainerRequest scRequest = recordFactory
          .newRecordInstance(StartContainerRequest.class);
      scRequest.setContainerLaunchContext(containerLaunchContext);
      scRequest.setContainer(container);
      try {
        proxy.startContainer(scRequest);
      } catch (Exception e) {
        LOG.info(StringUtils.stringifyException(e));
        Assert.assertTrue("Error, exception does not contain: "
            + EXCEPTION_CAUSE,
            e.getCause().getMessage().contains(EXCEPTION_CAUSE));

        return;
      }
    } finally {
      server.stop();
    }

    Assert.fail("timeout exception should have occurred!");
  }

  public class DummyContainerManager implements ContainerManager {

    private ContainerStatus status = null;

    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request) throws YarnRemoteException {
      GetContainerStatusResponse response = recordFactory
          .newRecordInstance(GetContainerStatusResponse.class);
      response.setStatus(status);
      return response;
    }

    @Override
    public StartContainerResponse startContainer(StartContainerRequest request)
        throws YarnRemoteException {
      StartContainerResponse response = recordFactory
          .newRecordInstance(StartContainerResponse.class);
      status = recordFactory.newRecordInstance(ContainerStatus.class);
      try {
        // make the thread sleep to look like its not going to respond
        Thread.sleep(10000);
      } catch (Exception e) {
        LOG.error(e);
        throw new UndeclaredThrowableException(e);
      }
      status.setState(ContainerState.RUNNING);
      status.setContainerId(request.getContainer().getId());
      status.setExitStatus(0);
      return response;
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest request)
        throws YarnRemoteException {
      Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw YarnRemoteExceptionFactoryProvider.getYarnRemoteExceptionFactory(
          null).createYarnRemoteException(e);
    }
  }
}
