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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AHSWebApp;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilterInitializer;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestApplicationHistoryServer {

  ApplicationHistoryServer historyServer = null;

  // simple test init/start/stop ApplicationHistoryServer. Status should change.
  @Test(timeout = 50000)
  public void testStartStopServer() throws Exception {
    historyServer = new ApplicationHistoryServer();
    Configuration config = new YarnConfiguration();
    historyServer.init(config);
    assertEquals(STATE.INITED, historyServer.getServiceState());
    assertEquals(5, historyServer.getServices().size());
    ApplicationHistoryClientService historyService =
        historyServer.getClientService();
    assertNotNull(historyServer.getClientService());
    assertEquals(STATE.INITED, historyService.getServiceState());

    historyServer.start();
    assertEquals(STATE.STARTED, historyServer.getServiceState());
    assertEquals(STATE.STARTED, historyService.getServiceState());
    historyServer.stop();
    assertEquals(STATE.STOPPED, historyServer.getServiceState());
  }

  // test launch method
  @Test(timeout = 60000)
  public void testLaunch() throws Exception {

    ExitUtil.disableSystemExit();
    try {
      historyServer =
          ApplicationHistoryServer.launchAppHistoryServer(new String[0]);
    } catch (ExitUtil.ExitException e) {
      assertEquals(0, e.status);
      ExitUtil.resetFirstExitException();
      fail();
    }
  }

  @Test(timeout = 50000)
  public void testFilteOverrides() throws Exception {

    HashMap<String, String> driver = new HashMap<String, String>();
    driver.put("", TimelineAuthenticationFilterInitializer.class.getName());
    driver.put(StaticUserWebFilter.class.getName(),
      TimelineAuthenticationFilterInitializer.class.getName() + ","
          + StaticUserWebFilter.class.getName());
    driver.put(AuthenticationFilterInitializer.class.getName(),
      TimelineAuthenticationFilterInitializer.class.getName());
    driver.put(TimelineAuthenticationFilterInitializer.class.getName(),
      TimelineAuthenticationFilterInitializer.class.getName());
    driver.put(AuthenticationFilterInitializer.class.getName() + ","
        + TimelineAuthenticationFilterInitializer.class.getName(),
      TimelineAuthenticationFilterInitializer.class.getName());
    driver.put(AuthenticationFilterInitializer.class.getName() + ", "
        + TimelineAuthenticationFilterInitializer.class.getName(),
      TimelineAuthenticationFilterInitializer.class.getName());

    for (Map.Entry<String, String> entry : driver.entrySet()) {
      String filterInitializer = entry.getKey();
      String expectedValue = entry.getValue();
      historyServer = new ApplicationHistoryServer();
      Configuration config = new YarnConfiguration();
      config.set("hadoop.http.filter.initializers", filterInitializer);
      historyServer.init(config);
      historyServer.start();
      Configuration tmp = historyServer.getConfig();
      assertEquals(expectedValue, tmp.get("hadoop.http.filter.initializers"));
      historyServer.stop();
      AHSWebApp.resetInstance();
    }
  }

  @After
  public void stop() {
    if (historyServer != null) {
      historyServer.stop();
    }
    AHSWebApp.resetInstance();
  }
}
