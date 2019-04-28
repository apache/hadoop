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

package org.apache.hadoop.yarn.server.webproxy.amfilter;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test AmIpFilter. Requests to a no declared hosts should has way through
 * proxy. Another requests can be filtered with (without) user name.
 *
 */
public class TestSecureAmFilter {

  private String proxyHost = "localhost";
  private static final File TEST_ROOT_DIR = new File("target",
      TestSecureAmFilter.class.getName() + "-root");
  private static File httpSpnegoKeytabFile = new File(
      KerberosTestUtils.getKeytabFile());
  private static Configuration rmconf = new Configuration();
  private static String httpSpnegoPrincipal = KerberosTestUtils
      .getServerPrincipal();
  private static boolean miniKDCStarted = false;
  private static MiniKdc testMiniKDC;

  @BeforeClass
  public static void setUp() {
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
    UserGroupInformation.setConfiguration(rmconf);
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), TEST_ROOT_DIR);
      setupKDC();
    } catch (Exception e) {
      assertTrue("Couldn't create MiniKDC", false);
    }
  }

  @AfterClass
  public static void tearDown() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }
  }

  private static void setupKDC() throws Exception {
    if (!miniKDCStarted) {
      testMiniKDC.start();
      getKdc().createPrincipal(httpSpnegoKeytabFile, "HTTP/localhost");
      miniKDCStarted = true;
    }
  }

  private static MiniKdc getKdc() {
    return testMiniKDC;
  }

  private class TestAmIpFilter extends AmIpFilter {

    private Set<String> proxyAddresses = null;

    protected Set<String> getProxyAddresses() {
      if (proxyAddresses == null) {
        proxyAddresses = new HashSet<String>();
      }
      proxyAddresses.add(proxyHost);
      return proxyAddresses;
    }
  }

  @Test
  public void testFindRedirectUrl() throws Exception {
    final String rm1 = "rm1";
    final String rm2 = "rm2";
    // generate a valid URL
    final String rm1Url = startSecureHttpServer();
    // invalid url
    final String rm2Url = "host2:8088";

    TestAmIpFilter filter = new TestAmIpFilter();
    TestAmIpFilter spy = Mockito.spy(filter);
    // make sure findRedirectUrl() go to HA branch
    spy.proxyUriBases = new HashMap<>();
    spy.proxyUriBases.put(rm1, rm1Url);
    spy.proxyUriBases.put(rm2, rm2Url);
    spy.rmUrls = new String[] {rm1, rm2};

    assertTrue(spy.isValidUrl(rm1Url));
    assertFalse(spy.isValidUrl(rm2Url));
    assertThat(spy.findRedirectUrl()).isEqualTo(rm1Url);
  }

  private String startSecureHttpServer() throws Exception {
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("test").setConf(rmconf)
        .addEndpoint(new URI("http://localhost")).setACL(
            new AccessControlList(rmconf.get(YarnConfiguration.YARN_ADMIN_ACL,
                YarnConfiguration.DEFAULT_YARN_ADMIN_ACL)));

    builder.setUsernameConfKey(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
        .setKeytabConfKey(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
        .setSecurityEnabled(UserGroupInformation.isSecurityEnabled());
    HttpServer2 server = builder.build();
    server.start();
    URL baseUrl = new URL(
        "http://" + NetUtils.getHostPortString(server.getConnectorAddress(0)));
    return baseUrl.toString();
  }
}
