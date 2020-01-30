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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

/**
 * Unit test for hosting web terminal servlet in node manager.
 */
public class TestNMWebTerminal {
  private static final File TESTROOTDIR = new File("target",
      TestNMWebServer.class.getSimpleName());
  private static final File TESTLOGDIR = new File("target",
      TestNMWebServer.class.getSimpleName() + "LogDir");
  private NodeHealthCheckerService healthChecker;
  private WebServer server;
  private int port;

  private NodeHealthCheckerService createNodeHealthCheckerService() {
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    return new NodeHealthCheckerService(dirsHandler);
  }

  private int startNMWebAppServer(String webAddr) {
    Configuration conf = new Configuration();
    Context nmContext = new NodeManager.NMContext(null, null, null, null,
        null, false, conf);
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
    conf.set(YarnConfiguration.NM_LOG_DIRS, TESTLOGDIR.getAbsolutePath());
    healthChecker = createNodeHealthCheckerService();
    healthChecker.init(conf);
    LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();
    conf.set(YarnConfiguration.NM_WEBAPP_ADDRESS, webAddr);
    server = new WebServer(nmContext, resourceView,
        new ApplicationACLsManager(conf), dirsHandler);
    server.init(conf);
    server.start();
    return server.getPort();
  }

  @Before
  public void setUp() {
    port = startNMWebAppServer("0.0.0.0:0");
  }

  @After
  public void tearDown() throws IOException {
    server.close();
    healthChecker.close();
  }

  @Test
  public void testWebTerminal() {
    Client client = Client.create();
    Builder builder = client.resource("http://127.0.0.1:" + port +
        "/terminal/terminal.template").accept("text/html");
    ClientResponse response = builder.get(ClientResponse.class);
    assertEquals(MediaType.TEXT_HTML + "; " + JettyUtils.UTF_8,
        response.getType().toString());
  }
}
