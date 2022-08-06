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
package org.apache.hadoop.yarn.server.router.secure;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.Router;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class AbstractSecureRouterTest {

  public static final String REALM = "EXAMPLE.COM";

  public static final String ROUTER = "router";
  public static final String LOCALHOST = "localhost";
  public static final String IP127001 = "127.0.0.1";
  public static final String ROUTER_LOCALHOST = "router/" + LOCALHOST;
  public static final String ROUTER_127001 = "router/" + IP127001;
  public static final String ROUTER_REALM = "router@" + REALM;
  public static final String ROUTER_LOCALHOST_REALM = ROUTER_LOCALHOST + "@" + REALM;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSecureRouterTest.class);

  public static final Configuration CONF;

  static {
    CONF = new Configuration();
    CONF.set("hadoop.security.authentication", "kerberos");
    CONF.setBoolean("hadoop.security.authorization", true);
  }

  public static final String SUN_SECURITY_KRB5_DEBUG = "sun.security.krb5.debug";

  public static final String CLIENT_RM_FEDERATION_CLIENT_INTERCEPTOR =
      "org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor";

  public static final String KERBEROS = "kerberos";

  protected static MiniKdc kdc;
  protected static File keytab_router;
  protected static File kdcWorkDir;
  protected static Properties kdcConf;

  protected Router router = null;

  protected static Configuration conf;

  @BeforeClass
  public static void beforeSecureRouterTestClass() throws Exception {

    // Sets up the KDC and Principals.
    setupKDCAndPrincipals();

    // Init YarnConfiguration
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.ROUTER_BIND_HOST, "0.0.0.0");
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        CLIENT_RM_FEDERATION_CLIENT_INTERCEPTOR);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, KERBEROS);
    conf.set(YarnConfiguration.ROUTER_PRINCIPAL, ROUTER_LOCALHOST_REALM);
    conf.set(YarnConfiguration.ROUTER_KEYTAB, keytab_router.getAbsolutePath());
  }

  /**
   * Sets up the KDC and Principals.
   *
   * @throws Exception an error occurred.
   */
  public static void setupKDCAndPrincipals() throws Exception {
    // set up the KDC
    File target = new File(System.getProperty("test.dir", "target"));
    kdcWorkDir = new File(target, "kdc");
    kdcWorkDir.mkdirs();
    if (!kdcWorkDir.mkdirs()) {
      Assert.assertTrue(kdcWorkDir.isDirectory());
    }
    kdcConf = MiniKdc.createConf();
    kdcConf.setProperty(MiniKdc.DEBUG, "true");
    kdc = new MiniKdc(kdcConf, kdcWorkDir);
    kdc.start();

    keytab_router = createKeytab(ROUTER, "router.keytab");
  }

  /**
   * Create the keytab for the given principal, includes
   * raw principal and $principal/localhost.
   *
   * @param principal principal short name.
   * @param filename filename of keytab.
   * @return file of keytab.
   * @throws Exception an error occurred.
   */
  public static File createKeytab(String principal, String filename) throws Exception {
    Assert.assertTrue("empty principal", StringUtils.isNotBlank(principal));
    Assert.assertTrue("empty host", StringUtils.isNotBlank(filename));
    Assert.assertNotNull("Null KDC", kdc);
    File keytab = new File(kdcWorkDir, filename);
    kdc.createPrincipal(keytab,
        principal,
        principal + "/localhost",
        principal + "/127.0.0.1");
    return keytab;
  }

  /**
   * Start the router in safe mode.
   *
   * @throws Exception an error occurred.
   */
  protected synchronized void startSecureRouter() throws Exception {
    Assert.assertNull("Router is already running", router);
    UserGroupInformation.setConfiguration(conf);
    router = new Router();
    router.init(conf);
    router.start();
  }

  /**
   * Shut down the KDC service.
   *
   * @throws Exception an error occurred.
   */
  public static void teardownKDC() throws Exception {
    if (kdc != null) {
      kdc.stop();
      kdc = null;
    }
  }

  /**
   * Stop the router in safe mode.
   *
   * @throws Exception an error occurred.
   */
  protected synchronized void stopSecureRouter() throws Exception {
    if (router != null) {
      router.stop();
      router = null;
    }
  }

  @After
  public void afterSecureRouterTest() throws Exception {
    LOG.info("teardown of router instance.");
    stopSecureRouter();
    teardownKDC();
  }
}
