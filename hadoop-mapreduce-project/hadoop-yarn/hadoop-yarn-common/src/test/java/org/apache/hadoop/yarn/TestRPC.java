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

import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.avro.ipc.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestRPC {

  private static final String EXCEPTION_MSG = "test error";
  private static final String EXCEPTION_CAUSE = "exception cause";
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
//  @Test
//  public void testAvroRPC() throws Exception {
//    test(AvroYarnRPC.class.getName());
//  }
//
//  @Test
//  public void testHadoopNativeRPC() throws Exception {
//    test(HadoopYarnRPC.class.getName());
//  }

  @Test
  public void testUnknownCall() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.IPC_RPC_IMPL, HadoopYarnProtoRPC.class
        .getName());
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    Server server = rpc.getServer(ContainerManager.class,
        new DummyContainerManager(), addr, conf, null, 1);
    server.start();

    // Any unrelated protocol would do
    ClientRMProtocol proxy = (ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, NetUtils.createSocketAddr("localhost:"
            + server.getPort()), conf);

    try {
      proxy.getNewApplication(Records
          .newRecord(GetNewApplicationRequest.class));
      Assert.fail("Excepted RPC call to fail with unknown method.");
    } catch (YarnRemoteException e) {
      Assert.assertEquals("Unknown method getNewApplication called on "
          + "org.apache.hadoop.yarn.proto.ClientRMProtocol"
          + "$ClientRMProtocolService$BlockingInterface protocol.", e
          .getMessage());
    }
  }

  @Test
  public void testHadoopProtoRPC() throws Exception {
    test(HadoopYarnProtoRPC.class.getName());
  }
  
  private void test(String rpcClass) throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.IPC_RPC_IMPL, rpcClass);
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    Server server = rpc.getServer(ContainerManager.class, 
            new DummyContainerManager(), addr, conf, null, 1);
    server.start();
    ContainerManager proxy = (ContainerManager) 
        rpc.getProxy(ContainerManager.class, 
            NetUtils.createSocketAddr("localhost:" + server.getPort()), conf);
    ContainerLaunchContext containerLaunchContext = 
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    containerLaunchContext.setUser("dummy-user");
    ContainerId containerId = 
        recordFactory.newRecordInstance(ContainerId.class);
    ApplicationId applicationId = 
        recordFactory.newRecordInstance(ApplicationId.class);
    ApplicationAttemptId applicationAttemptId =
        recordFactory.newRecordInstance(ApplicationAttemptId.class);
    applicationId.setClusterTimestamp(0);
    applicationId.setId(0);
    applicationAttemptId.setApplicationId(applicationId);
    applicationAttemptId.setAttemptId(0);
    containerId.setApplicationAttemptId(applicationAttemptId);
    containerId.setId(100);
    containerLaunchContext.setContainerId(containerId);
    containerLaunchContext.setResource(
        recordFactory.newRecordInstance(Resource.class));
//    containerLaunchContext.env = new HashMap<CharSequence, CharSequence>();
//    containerLaunchContext.command = new ArrayList<CharSequence>();
    
    StartContainerRequest scRequest = 
        recordFactory.newRecordInstance(StartContainerRequest.class);
    scRequest.setContainerLaunchContext(containerLaunchContext);
    proxy.startContainer(scRequest);
    
    GetContainerStatusRequest gcsRequest = 
        recordFactory.newRecordInstance(GetContainerStatusRequest.class);
    gcsRequest.setContainerId(containerLaunchContext.getContainerId());
    GetContainerStatusResponse response =  proxy.getContainerStatus(gcsRequest);
    ContainerStatus status = response.getStatus();
    
    //test remote exception
    boolean exception = false;
    try {
      StopContainerRequest stopRequest = recordFactory.newRecordInstance(StopContainerRequest.class);
      stopRequest.setContainerId(containerLaunchContext.getContainerId());
      proxy.stopContainer(stopRequest);
    } catch (YarnRemoteException e) {
      exception = true;
      System.err.println(e.getMessage());
      System.err.println(e.getCause().getMessage());
      Assert.assertTrue(EXCEPTION_MSG.equals(e.getMessage()));
      Assert.assertTrue(EXCEPTION_CAUSE.equals(e.getCause().getMessage()));
      System.out.println("Test Exception is " + RPCUtil.toString(e));
    }
    Assert.assertTrue(exception);
    
    server.close();
    Assert.assertNotNull(status);
    Assert.assertEquals(ContainerState.RUNNING, status.getState());
  }

  public class DummyContainerManager implements ContainerManager {

    private ContainerStatus status = null;    
    
    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request)
    throws YarnRemoteException {
      GetContainerStatusResponse response = 
          recordFactory.newRecordInstance(GetContainerStatusResponse.class);
      response.setStatus(status);
      return response;
    }

    @Override
    public StartContainerResponse startContainer(StartContainerRequest request) 
        throws YarnRemoteException {
      ContainerLaunchContext container = request.getContainerLaunchContext();
      StartContainerResponse response = 
          recordFactory.newRecordInstance(StartContainerResponse.class);
      status = recordFactory.newRecordInstance(ContainerStatus.class);
      status.setState(ContainerState.RUNNING);
      status.setContainerId(container.getContainerId());
      status.setExitStatus(0);
      return response;
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest request) 
    throws YarnRemoteException {
      Exception e = new Exception(EXCEPTION_MSG, 
          new Exception(EXCEPTION_CAUSE));
      throw YarnRemoteExceptionFactoryProvider
          .getYarnRemoteExceptionFactory(null).createYarnRemoteException(e);
    }
  }
}
