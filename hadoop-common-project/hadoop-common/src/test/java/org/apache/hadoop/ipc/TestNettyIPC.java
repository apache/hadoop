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

package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for IPC. */
public class TestNettyIPC extends TestIPC {
  public static final Logger LOG = LoggerFactory.getLogger(TestNettyIPC.class);

  @Before
  public void setupConf() {
    conf = newConfiguration();
    Client.setPingInterval(conf, PING_INTERVAL);
    // tests may enable security, so disable before each test
    UserGroupInformation.setConfiguration(conf);
  }

  static Configuration newConfiguration() {
    Configuration confLocal = new Configuration();
    confLocal.setBoolean(
        CommonConfigurationKeys.IPC_SERVER_NETTY_ENABLE_KEY, true);
    confLocal.setBoolean(
        CommonConfigurationKeys.IPC_CLIENT_NETTY_ENABLE_KEY, true);
    return confLocal;
  }

  @Override
  public void testIpcWithReaderQueuing() {}

  @Override
  public void testIpcFromHadoop_0_18_13() {}

  @Override
  public void testIpcFromHadoop0_20_3() {}

  @Override
  public void testIpcFromHadoop0_21_0() {}

  @Override
  public void testHttpGetResponse() {}

  @Override
  public void testInsecureVersionMismatch() {}

  @Override
  public void testSecureVersionMismatch() {}
}
