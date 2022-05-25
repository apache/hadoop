/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.router.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.Router;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.Test;
import org.apache.hadoop.security.authentication.KerberosTestUtils;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestSecureLogin {

  private static final File TEST_ROOT_DIR = new File("target",
      TestSecureLogin.class.getName() + "-root");
  private static File routerKeytabFile = new File(
      KerberosTestUtils.getKeytabFile());

  private static MiniKdc testMiniKDC;

  @BeforeClass
  public static void setUp() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), TEST_ROOT_DIR);
      testMiniKDC.start();
      testMiniKDC.createPrincipal(routerKeytabFile, "yarn/localhost");
    } catch (Exception e) {
      fail("Couldn't setup MiniKDC");
    }
  }

  @Test
  public void testRouterSecureLogin() {
    Router router = null;
    try {
      Configuration conf = new YarnConfiguration();
      conf.set(YarnConfiguration.ROUTER_BIND_HOST, "0.0.0.0");
      conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
          "org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor");
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      conf.set("yarn.router.principal", "yarn/localhost@EXAMPLE.COM");
      conf.set("yarn.router.keytab", routerKeytabFile.getAbsolutePath());
      assertEquals(AuthenticationMethod.SIMPLE,
          UserGroupInformation.getLoginUser().getAuthenticationMethod());
      UserGroupInformation.setConfiguration(conf);
      router = new Router();
      router.init(conf);
      router.start();
      assertEquals(AuthenticationMethod.KERBEROS,
          UserGroupInformation.getLoginUser().getAuthenticationMethod());
      assertEquals("yarn/localhost@EXAMPLE.COM",
          UserGroupInformation.getLoginUser().getUserName());
    } catch (Throwable t) {
      fail("Can't start router!");
    } finally {
      if (router != null) {
        router.stop();
      }
    }
  }

  @AfterClass
  public static void cleanUp() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
      testMiniKDC = null;
    }
  }
}
