/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test class for {@link SCMSecurityProtocolServer}.
 * */
public class TestSCMSecurityProtocolServer {
  private SCMSecurityProtocolServer securityProtocolServer;
  private OzoneConfiguration config;

  @Rule
  public Timeout timeout = new Timeout(1000 * 20);

  @Before
  public void setUp() throws Exception {
    config = new OzoneConfiguration();
    config.set(OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
        OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT + ":0");
    securityProtocolServer = new SCMSecurityProtocolServer(config, null);
  }

  @After
  public void tearDown() {
    if (securityProtocolServer != null) {
      securityProtocolServer.stop();
      securityProtocolServer = null;
    }
    config = null;
  }

  @Test
  public void testStart() {
    securityProtocolServer.start();
  }

  @Test
  public void testStop() {
    securityProtocolServer.stop();
  }
}