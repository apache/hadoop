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

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Lists;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN;
import static org.junit.Assert.assertEquals;

public class TestServerAuthMethods extends TestRpcBase {

  private Server server;
  private TestRpcService proxy;

  @Before
  public void setup() {
    server = null;
    proxy = null;
    conf = new Configuration();
    RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine2.class);
  }

  @Test
  public void testSimpleAuthServer() throws IOException {
    try {
      server = setupTestServer(conf, 1);
      assertEquals(Lists.newArrayList(SIMPLE), server.getEnabledAuthMethods());
    } finally {
      stop(server, proxy);
    }
  }

  @Test
  public void testKerberosAuthServer() throws IOException {
    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.toString());

    try {
      server = setupTestServer(conf, 1);
      assertEquals(Lists.newArrayList(KERBEROS), server.getEnabledAuthMethods());
    } finally {
      stop(server, proxy);
    }
  }

  @Test
  public void testKerberosAuthServerWithSecretManager() throws IOException {
    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.toString());
    TestTokenSecretManager sm = new TestTokenSecretManager();

    try {
      server = setupTestServer(conf, 1, sm);
      assertEquals(Lists.newArrayList(TOKEN, KERBEROS), server.getEnabledAuthMethods());
    } finally {
      stop(server, proxy);
    }
  }

  @Test
  public void testKerberosAuthServerWithSecretManagerMigrationMode() throws IOException {
    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.toString());
    conf.setBoolean("dfs.datanode.block.access.token.unsafe.allowed-not-required", true);
    TestTokenSecretManager sm = new TestTokenSecretManager();

    try {
      server = setupTestServer(conf, 1, sm);
      assertEquals(Lists.newArrayList(KERBEROS), server.getEnabledAuthMethods());
    } finally {
      stop(server, proxy);
    }
  }

}
