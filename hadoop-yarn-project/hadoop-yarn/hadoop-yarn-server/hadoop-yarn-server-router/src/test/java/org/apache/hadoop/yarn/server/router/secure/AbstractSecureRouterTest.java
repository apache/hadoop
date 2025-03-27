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
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractSecureRouterTest {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSecureRouterTest.class);

  ////////////////////////////////
  // Kerberos Constants
  ////////////////////////////////

  public static final String REALM = "EXAMPLE.COM";
  public static final String ROUTER = "router";
  public static final String LOCALHOST = "localhost";
  public static final String IP127001 = "127.0.0.1";
  public static final String ROUTER_LOCALHOST = "router/" + LOCALHOST;
  public static final String ROUTER_127001 = "router/" + IP127001;
  public static final String ROUTER_REALM = "router@" + REALM;
  public static final String ROUTER_LOCALHOST_REALM = ROUTER_LOCALHOST + "@" + REALM;
  public static final String SUN_SECURITY_KRB5_DEBUG = "sun.security.krb5.debug";
  public static final String KERBEROS = "kerberos";

  ////////////////////////////////
  // BeforeSecureRouterTestClass Init
  ////////////////////////////////

  private static MiniKdc kdc;
  private static File routerKeytab;
  private static File kdcWorkDir;
  private static Configuration conf;

  ////////////////////////////////
  // Specific Constant
  // Like Mem, VCore, ClusterNum
  ////////////////////////////////
  private static final int NUM_SUBCLUSTER = 4;
  private static final int GB = 1024;
  private static final int NM_MEMORY = 8 * GB;
  private static final int NM_VCORE = 4;

  ////////////////////////////////
  // Test use in subclasses
  ////////////////////////////////

  private Router router = null;

  private static ConcurrentHashMap<SubClusterId, MockRM> mockRMs =
      new ConcurrentHashMap<>();

  @BeforeClass
  public static void beforeSecureRouterTestClass() throws Exception {
    // Sets up the KDC and Principals.
    setupKDCAndPrincipals();

    // Init YarnConfiguration
    conf = new YarnConfiguration();

    // Enable Kerberos authentication configuration
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, KERBEROS);

    // Router configuration
    conf.set(YarnConfiguration.ROUTER_BIND_HOST, "0.0.0.0");
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        FederationClientInterceptor.class.getName());

    // Router Kerberos KeyTab configuration
    conf.set(YarnConfiguration.ROUTER_PRINCIPAL, ROUTER_LOCALHOST_REALM);
    conf.set(YarnConfiguration.ROUTER_KEYTAB, routerKeytab.getAbsolutePath());

    DefaultMetricsSystem.setMiniClusterMode(true);
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
      assertTrue(kdcWorkDir.isDirectory());
    }
    Properties kdcConf = MiniKdc.createConf();
    kdcConf.setProperty(MiniKdc.DEBUG, "true");
    kdc = new MiniKdc(kdcConf, kdcWorkDir);
    kdc.start();
    routerKeytab = createKeytab(ROUTER, "router.keytab");
  }

  /**
   * Initialize RM in safe mode.
   *
   * @throws Exception an error occurred.
   */
  public static void setupSecureMockRM() throws Exception {
    for (int i = 0; i < NUM_SUBCLUSTER; i++) {
      SubClusterId sc = SubClusterId.newInstance(i);
      if (mockRMs.containsKey(sc)) {
        continue;
      }
      MockRM mockRM = new TestRMRestart.TestSecurityMockRM(conf);
      mockRM.start();
      mockRM.registerNode("127.0.0.1:1234", NM_MEMORY, NM_VCORE);
      mockRMs.put(sc, mockRM);
    }
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
    assertTrue("empty principal", StringUtils.isNotBlank(principal));
    assertTrue("empty host", StringUtils.isNotBlank(filename));
    assertNotNull("null KDC", kdc);
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
  public synchronized void startSecureRouter() {
    assertNull("Router is already running", router);
    MemoryFederationStateStore stateStore = new MemoryFederationStateStore();
    stateStore.init(getConf());
    FederationStateStoreFacade.getInstance(getConf()).reinitialize(stateStore, getConf());
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

  /**
   * Stop the entire test service.
   *
   * @throws Exception an error occurred.
   */
  @AfterClass
  public static void afterSecureRouterTest() throws Exception {
    LOG.info("teardown of kdc instance.");
    teardownKDC();
    if (mockRMs != null && mockRMs.isEmpty()) {
      for (MockRM mockRM : mockRMs.values()) {
        mockRM.close();
      }
    }
    mockRMs.clear();
  }

  public static MiniKdc getKdc() {
    return kdc;
  }

  public Router getRouter() {
    return router;
  }

  public static ConcurrentHashMap<SubClusterId, MockRM> getMockRMs() {
    return mockRMs;
  }

  public static Configuration getConf() {
    return conf;
  }
}
