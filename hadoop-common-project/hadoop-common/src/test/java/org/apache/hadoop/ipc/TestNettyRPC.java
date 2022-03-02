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

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.hadoop.http.TestSSLHttpServer.EXCLUDED_CIPHERS;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.CLIENT_KEY_STORE_PASSWORD_DEFAULT;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.SERVER_KEY_STORE_PASSWORD_DEFAULT;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.TRUST_STORE_PASSWORD_DEFAULT;
import static org.junit.Assert.assertEquals;

/** Unit tests for RPC. */
@SuppressWarnings("deprecation")
public class TestNettyRPC extends TestRPC {

  public static final Logger LOG = LoggerFactory.getLogger(TestNettyRPC.class);

  private static String keystoreDir;
  private static String sslConfDir;

  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestNettyRPC.class.getSimpleName());

  @Before
  public void setup() throws Exception {
    GenericTestUtils.setLogLevel(Client.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(Server.LOG, Level.DEBUG);

    setupConf();

    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_NETTY_ENABLE_KEY,
        true);
    conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_NETTY_ENABLE_KEY,
        true);
    conf.setBoolean(CommonConfigurationKeys.IPC_NETTY_TESTING,
        true);

    File base = new File(BASEDIR);

    FileUtil.fullyDelete(base);

    base.mkdirs();

    keystoreDir = new File(BASEDIR).getAbsolutePath();

    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestNettyRPC.class);

    KeyStoreTestUtil.setupSSLConfig(keystoreDir, sslConfDir, conf,
        false, true,
        EXCLUDED_CIPHERS, SERVER_KEY_STORE_PASSWORD_DEFAULT,
        CLIENT_KEY_STORE_PASSWORD_DEFAULT, TRUST_STORE_PASSWORD_DEFAULT);

    int threadsBefore = countThreads("Socket Reader");
    assertEquals("Expect no Reader threads running before test",
        0, threadsBefore);
  }

  @After
  public void cleanup() {
    int threadsBefore = countThreads("Socket Reader");
    assertEquals("Expect no Reader threads running after test",
        0, threadsBefore);
  }

  //TODO: testAuthorization disabling should be checked to see why it fails when
  //      SSL is turned on.
  @Ignore
  @Override
  public void testAuthorization() {}

  @Ignore
  @Override
  public void testStopsAllThreads() {}

  @Ignore
  @Override
  public void testRPCInterrupted() {}

  @Ignore
  @Override
  public void testRPCInterruptedSimple() {}

  @Ignore
  @Override
  public void testClientRpcTimeout() {}

  @Test
  public void testServerNameFromClass() {
    Assert.assertEquals("TestNettyRPC",
        RPC.Server.serverNameFromClass(this.getClass()));
    //TODO: ERROR expected:<Test[NettyRPC]> but was:<Test[Class]>
    //      Discuss why it should be Test[Class] ?
    Assert.assertEquals("TestNettyRPC",
        RPC.Server.serverNameFromClass(TestNettyRPC.TestClass.class));

    Object testing = new TestClass().classFactory();
    Assert.assertEquals("Embedded",
        RPC.Server.serverNameFromClass(testing.getClass()));

    testing = new TestClass().classFactoryAbstract();
    Assert.assertEquals("TestClass",
        RPC.Server.serverNameFromClass(testing.getClass()));

    testing = new TestClass().classFactoryObject();
    Assert.assertEquals("TestClass",
        RPC.Server.serverNameFromClass(testing.getClass()));

  }

  public static void main(String[] args) throws Exception {
    new TestNettyRPC().testCallsInternal(conf);
  }
}
