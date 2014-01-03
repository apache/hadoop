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

package org.apache.hadoop.yarn.server.webproxy;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestWebAppProxyServer {
  private WebAppProxyServer webAppProxy = null;
  private final String proxyAddress = "0.0.0.0:8888";

  @Before
  public void setUp() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.PROXY_ADDRESS, proxyAddress);
    webAppProxy = new WebAppProxyServer();
    webAppProxy.init(conf);
  }

  @After
  public void tearDown() throws Exception {
    webAppProxy.stop();
  }

  @Test
  public void testStart() {
    assertEquals(STATE.INITED, webAppProxy.getServiceState());
    webAppProxy.start();
    for (Service service : webAppProxy.getServices()) {
      if (service instanceof WebAppProxy) {
        assertEquals(((WebAppProxy) service).getBindAddress(), proxyAddress);
      }
    }
    assertEquals(STATE.STARTED, webAppProxy.getServiceState());
  }
}
