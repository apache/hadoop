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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.mockito.Mockito.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Test class for Node Manager Container Web Socket.
 */
public class TestNMContainerWebSocket {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestNMContainerWebSocket.class);

  private static final File TESTROOTDIR = new File("target",
      TestNMWebServer.class.getSimpleName());
  private static File testLogDir = new File("target",
      TestNMWebServer.class.getSimpleName() + "LogDir");
  private WebServer server;

  @Before
  public void setup() {
    TESTROOTDIR.mkdirs();
    testLogDir.mkdir();
  }

  @After
  public void tearDown() {
    FileUtil.fullyDelete(TESTROOTDIR);
    FileUtil.fullyDelete(testLogDir);
  }

  private int startNMWebAppServer(String webAddr) {
    Configuration conf = new Configuration();
    Context nmContext = new NodeManager.NMContext(null, null, null, null, null,
        false, conf);
    ResourceView resourceView = new ResourceView() {
      @Override
      public long getVmemAllocatedForContainers() {
        return 0;
      }

      @Override
      public long getPmemAllocatedForContainers() {
        return 0;
      }

      @Override
      public long getVCoresAllocatedForContainers() {
        return 0;
      }

      @Override
      public boolean isVmemCheckEnabled() {
        return true;
      }

      @Override
      public boolean isPmemCheckEnabled() {
        return true;
      }
    };
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, TESTROOTDIR.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOG_DIRS, testLogDir.getAbsolutePath());
    NodeHealthCheckerService healthChecker = createNodeHealthCheckerService();
    healthChecker.init(conf);
    LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();
    conf.set(YarnConfiguration.NM_WEBAPP_ADDRESS, webAddr);
    server = new WebServer(nmContext, resourceView,
        new ApplicationACLsManager(conf), dirsHandler);
    try {
      server.init(conf);
      server.start();
      return server.getPort();
    } finally {
    }
  }

  private NodeHealthCheckerService createNodeHealthCheckerService() {
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    return new NodeHealthCheckerService(dirsHandler);
  }

  @Test
  public void testWebServerWithServlet() {
    int port = startNMWebAppServer("0.0.0.0");
    LOG.info("bind to port: " + port);
    StringBuilder sb = new StringBuilder();
    sb.append("ws://localhost:").append(port).append("/container/abc/");
    String dest = sb.toString();
    WebSocketClient client = new WebSocketClient();
    try {
      ContainerShellClientSocketTest socket = new ContainerShellClientSocketTest();
      client.start();
      URI echoUri = new URI(dest);
      Future<Session> future = client.connect(socket, echoUri);
      Session session = future.get();
      session.getRemote().sendString("hello world");
      session.close();
      client.stop();
    } catch (Throwable t) {
      LOG.error("Failed to connect WebSocket and send message to server", t);
    } finally {
      try {
        client.stop();
        server.close();
      } catch (Exception e) {
        LOG.error("Failed to close client", e);
      }
    }
  }

  @Test
  public void testContainerShellWebSocket() {
    Context nm = mock(Context.class);
    Session session = mock(Session.class);
    Container container = mock(Container.class);
    UpgradeRequest request = mock(UpgradeRequest.class);
    ApplicationACLsManager aclManager = mock(ApplicationACLsManager.class);
    ContainerShellWebSocket.init(nm);
    ContainerShellWebSocket ws = new ContainerShellWebSocket();
    List<String> names = new ArrayList<>();
    names.add("foobar");
    Map<String, List<String>> mockParameters = new HashMap<>();
    mockParameters.put("user.name", names);
    when(session.getUpgradeRequest()).thenReturn(request);
    when(request.getParameterMap()).thenReturn(mockParameters);
    when(container.getUser()).thenReturn("foobar");
    when(nm.getApplicationACLsManager()).thenReturn(aclManager);
    when(aclManager.areACLsEnabled()).thenReturn(false);
    try {
      boolean authorized = ws.checkAuthorization(session, container);
      Assert.assertTrue("Not authorized", authorized);
    } catch (IOException e) {
      Assert.fail("Should not throw exception.");
    }
  }
}
