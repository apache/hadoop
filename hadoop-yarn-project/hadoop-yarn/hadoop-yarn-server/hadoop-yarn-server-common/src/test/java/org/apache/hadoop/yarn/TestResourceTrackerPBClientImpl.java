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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test ResourceTrackerPBClientImpl. this class should have methods
 * registerNodeManager and newRecordInstance.
 */
public class TestResourceTrackerPBClientImpl {

  private static ResourceTracker client;
  private static Server server;
  private final static org.apache.hadoop.yarn.factories.RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @BeforeClass
  public static void start() {
    InetSocketAddress address = new InetSocketAddress(0);
    Configuration configuration = new Configuration();
    ResourceTracker instance = new ResourceTrackerTestImpl();
    server = RpcServerFactoryPBImpl.get().getServer(ResourceTracker.class,
        instance, address, configuration, null, 1);
    server.start();

    client = (ResourceTracker) RpcClientFactoryPBImpl.get().getClient(
        ResourceTracker.class, 1, NetUtils.getConnectAddress(server),
        configuration);

  }

  @AfterClass
  public static void stop() {
    if (server != null) {
      server.stop();
    }
  }

  /**
   * Test the method registerNodeManager. Method should return a not null
   * result.
   * 
   */
  @Test
  public void testResourceTrackerPBClientImpl() throws Exception {
    RegisterNodeManagerRequest request = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    assertNotNull(client.registerNodeManager(request));
    
    ResourceTrackerTestImpl.exception = true;
    try {
      client.registerNodeManager(request);
      fail("there  should be YarnException");
    } catch (YarnException e) {
      assertTrue(e.getMessage().startsWith("testMessage"));
    }finally{
      ResourceTrackerTestImpl.exception = false;
    }

  }

  /**
   * Test the method nodeHeartbeat. Method should return a not null result.
   * 
   */

  @Test
  public void testNodeHeartbeat() throws Exception {
    NodeHeartbeatRequest request = recordFactory
        .newRecordInstance(NodeHeartbeatRequest.class);
    assertNotNull(client.nodeHeartbeat(request));
    
    ResourceTrackerTestImpl.exception = true;
    try {
      client.nodeHeartbeat(request);
      fail("there  should be YarnException");
    } catch (YarnException e) {
      assertTrue(e.getMessage().startsWith("testMessage"));
    }finally{
      ResourceTrackerTestImpl.exception = false;
    }

  }

  

  public static class ResourceTrackerTestImpl implements ResourceTracker {

    public static boolean exception = false;

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException, IOException {
      if (exception) {
        throw new YarnException("testMessage");
      }
      return recordFactory.newRecordInstance(RegisterNodeManagerResponse.class);
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      if (exception) {
        throw new YarnException("testMessage");
      }
      return recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
    }

  }
}
