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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.File;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test TestWebApps.
 *
 */
public class TestWebApps {

  private static final File TEST_ROOT_DIR = new File("target",
      TestWebApps.class.getName() + "-root");
  private static File httpSpnegoKeytabFile = new File(
      KerberosTestUtils.getKeytabFile());
  private static String httpSpnegoPrincipal = KerberosTestUtils
      .getServerPrincipal();
  private static boolean miniKDCStarted = false;
  private static MiniKdc testMiniKDC;
  private static MockRM rm;

  @BeforeClass
  public static void setUp() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), TEST_ROOT_DIR);
      setupKDC();
    } catch (Exception e) {
      assertTrue("Couldn't create MiniKDC:" + e.getMessage(), false);
    }
    setupAndStartRM();
  }

  private static void setupAndStartRM() {
    Configuration rmconf = new Configuration();
    rmconf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    rmconf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    rmconf.setBoolean(YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER,
        true);
    rmconf.set("hadoop.http.filter.initializers",
        RMAuthenticationFilterInitializer.class.getName());
    rmconf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY,
        httpSpnegoPrincipal);
    rmconf.set(YarnConfiguration.RM_KEYTAB,
        httpSpnegoKeytabFile.getAbsolutePath());
    rmconf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
        httpSpnegoKeytabFile.getAbsolutePath());

    rmconf.setBoolean(MockRM.ENABLE_WEBAPP, true);
    UserGroupInformation.setConfiguration(rmconf);
    rm = new MockRM(rmconf);
    rm.start();
  }

  @AfterClass
  public static void tearDown() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }
    if (rm != null) {
      rm.stop();
    }
  }

  private static void setupKDC() throws Exception {
    if (!miniKDCStarted) {
      testMiniKDC.start();
      getKdc().createPrincipal(httpSpnegoKeytabFile, "HTTP/localhost",
          UserGroupInformation.getLoginUser().getShortUserName());
      miniKDCStarted = true;
    }
  }

  private static MiniKdc getKdc() {
    return testMiniKDC;
  }

  @Test
  public void testWebAppInitializersOrdering() throws Exception {
    String[] filterNamesArray = {"NoCacheFilter", "safety",
        "RMAuthenticationFilter", "SpnegoFilter",
        "org.apache.hadoop.security.http.XFrameOptionsFilter", "guice"};

    HttpServer2 server = rm.getWebapp().httpServer();
    WebAppContext context = server.getWebAppContext();
    FilterHolder[] filterHolders = context.getServletHandler().getFilters();
    Assert.assertEquals(filterHolders.length, 6);
    for (int index = 0; index < filterHolders.length; index++) {
      Assert.assertTrue(
          filterHolders[index].getName().equals(filterNamesArray[index]));
    }
  }
}
