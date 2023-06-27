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

package org.apache.hadoop.yarn.server.globalpolicygenerator.secure;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractGlobalPolicyGeneratorTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractGlobalPolicyGeneratorTest.class);

  ////////////////////////////////
  // Kerberos Constants
  ////////////////////////////////

  public static final String REALM = "EXAMPLE.COM";
  public static final String GPG = "gpg";
  public static final String LOCALHOST = "localhost";
  public static final String IP127001 = "127.0.0.1";
  public static final String GPG_LOCALHOST = "gpg/" + LOCALHOST;
  public static final String GPG_LOCALHOST_REALM = GPG_LOCALHOST + "@" + REALM;
  public static final String SUN_SECURITY_KRB5_DEBUG = "sun.security.krb5.debug";
  public static final String KERBEROS = "kerberos";

  ////////////////////////////////
  // BeforeSecureRouterTestClass Init
  ////////////////////////////////

  private static MiniKdc kdc;
  private static File routerKeytab;
  private static File kdcWorkDir;
  private static Configuration conf;
  private GlobalPolicyGenerator gpg;

  @BeforeClass
  public static void beforeSecureRouterTestClass() throws Exception {
    // Sets up the KDC and Principals.
    setupKDCAndPrincipals();

    // Init YarnConfiguration
    conf = new YarnConfiguration();

    // Enable Kerberos authentication configuration
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, KERBEROS);

    // Router Kerberos KeyTab configuration
    conf.set(YarnConfiguration.GPG_PRINCIPAL, GPG_LOCALHOST_REALM);
    conf.set(YarnConfiguration.GPG_KEYTAB, routerKeytab.getAbsolutePath());

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
    routerKeytab = createKeytab(GPG, "gpg.keytab");
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
  public synchronized void startSecureGPG() {
    assertNull("GPG is already running", gpg);
    MemoryFederationStateStore stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);
    FederationStateStoreFacade.getInstance().reinitialize(stateStore, conf);
    UserGroupInformation.setConfiguration(conf);
    gpg = new GlobalPolicyGenerator();
    gpg.init(conf);
    gpg.start();
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

  public GlobalPolicyGenerator getGpg() {
    return gpg;
  }

  public static MiniKdc getKdc() {
    return kdc;
  }

  /**
   * Stop the router in safe mode.
   *
   * @throws Exception an error occurred.
   */
  protected synchronized void stopSecureRouter() throws Exception {
    if (gpg != null) {
      gpg.stop();
      gpg = null;
    }
  }
}
