/*
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

package org.apache.hadoop.registry.secure;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.registry.RegistryTestHelper;
import org.apache.hadoop.registry.client.impl.zk.RegistrySecurity;
import org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions;
import org.apache.hadoop.registry.server.services.AddingCompositeService;
import org.apache.hadoop.registry.server.services.MicroZookeeperService;
import org.apache.hadoop.registry.server.services.MicroZookeeperServiceKeys;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Add kerberos tests. This is based on the (JUnit3) KerberosSecurityTestcase
 * and its test case, <code>TestMiniKdc</code>
 */
public class AbstractSecureRegistryTest extends RegistryTestHelper {
  public static final String REALM = "EXAMPLE.COM";
  public static final String ZOOKEEPER = "zookeeper";
  public static final String ZOOKEEPER_LOCALHOST = "zookeeper/localhost";
  public static final String ZOOKEEPER_1270001 = "zookeeper/127.0.0.1";
  public static final String ZOOKEEPER_REALM = "zookeeper@" + REALM;
  public static final String ZOOKEEPER_CLIENT_CONTEXT = ZOOKEEPER;
  public static final String ZOOKEEPER_SERVER_CONTEXT = "ZOOKEEPER_SERVER";
  ;
  public static final String ZOOKEEPER_LOCALHOST_REALM =
      ZOOKEEPER_LOCALHOST + "@" + REALM;
  public static final String ALICE = "alice";
  public static final String ALICE_CLIENT_CONTEXT = "alice";
  public static final String ALICE_LOCALHOST = "alice/localhost";
  public static final String BOB = "bob";
  public static final String BOB_CLIENT_CONTEXT = "bob";
  public static final String BOB_LOCALHOST = "bob/localhost";


  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSecureRegistryTest.class);

  public static final Configuration CONF;

  static {
    CONF = new Configuration();
    CONF.set("hadoop.security.authentication", "kerberos");
    CONF.setBoolean("hadoop.security.authorization", true);
  }

  private static final AddingCompositeService classTeardown =
      new AddingCompositeService("classTeardown");

  // static initializer guarantees it is always started
  // ahead of any @BeforeClass methods
  static {
    classTeardown.init(CONF);
    classTeardown.start();
  }

  public static final String SUN_SECURITY_KRB5_DEBUG =
      "sun.security.krb5.debug";

  private final AddingCompositeService teardown =
      new AddingCompositeService("teardown");

  protected static MiniKdc kdc;
  protected static File keytab_zk;
  protected static File keytab_bob;
  protected static File keytab_alice;
  protected static File kdcWorkDir;
  protected static Properties kdcConf;
  protected static RegistrySecurity registrySecurity;

  @Rule
  public final Timeout testTimeout = new Timeout(900000);

  @Rule
  public TestName methodName = new TestName();
  protected MicroZookeeperService secureZK;
  protected static File jaasFile;
  private LoginContext zookeeperLogin;
  private static String zkServerPrincipal;

  /**
   * All class initialization for this test class
   * @throws Exception
   */
  @BeforeClass
  public static void beforeSecureRegistryTestClass() throws Exception {
    registrySecurity = new RegistrySecurity("registrySecurity");
    registrySecurity.init(CONF);
    setupKDCAndPrincipals();
    RegistrySecurity.clearJaasSystemProperties();
    RegistrySecurity.bindJVMtoJAASFile(jaasFile);
    initHadoopSecurity();
  }

  @AfterClass
  public static void afterSecureRegistryTestClass() throws
      Exception {
    describe(LOG, "teardown of class");
    classTeardown.close();
    teardownKDC();
  }

  /**
   * give our thread a name
   */
  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * For unknown reasons, the before-class setting of the JVM properties were
   * not being picked up. This method addresses that by setting them
   * before every test case
   */
  @Before
  public void beforeSecureRegistryTest() {

  }

  @After
  public void afterSecureRegistryTest() throws IOException {
    describe(LOG, "teardown of instance");
    teardown.close();
    stopSecureZK();
  }

  protected static void addToClassTeardown(Service svc) {
    classTeardown.addService(svc);
  }

  protected void addToTeardown(Service svc) {
    teardown.addService(svc);
  }


  public static void teardownKDC() throws Exception {
    if (kdc != null) {
      kdc.stop();
      kdc = null;
    }
  }

  /**
   * Sets up the KDC and a set of principals in the JAAS file
   *
   * @throws Exception
   */
  public static void setupKDCAndPrincipals() throws Exception {
    // set up the KDC
    File target = new File(System.getProperty("test.dir", "target"));
    kdcWorkDir = new File(target, "kdc");
    kdcWorkDir.mkdirs();
    if (!kdcWorkDir.mkdirs()) {
      assertTrue(kdcWorkDir.isDirectory());
    }
    kdcConf = MiniKdc.createConf();
    kdcConf.setProperty(MiniKdc.DEBUG, "true");
    kdc = new MiniKdc(kdcConf, kdcWorkDir);
    kdc.start();

    keytab_zk = createKeytab(ZOOKEEPER, "zookeeper.keytab");
    keytab_alice = createKeytab(ALICE, "alice.keytab");
    keytab_bob = createKeytab(BOB, "bob.keytab");
    zkServerPrincipal = Shell.WINDOWS ? ZOOKEEPER_1270001 : ZOOKEEPER_LOCALHOST;

    StringBuilder jaas = new StringBuilder(1024);
    jaas.append(registrySecurity.createJAASEntry(ZOOKEEPER_CLIENT_CONTEXT,
        ZOOKEEPER, keytab_zk));
    jaas.append(registrySecurity.createJAASEntry(ZOOKEEPER_SERVER_CONTEXT,
        zkServerPrincipal, keytab_zk));
    jaas.append(registrySecurity.createJAASEntry(ALICE_CLIENT_CONTEXT,
        ALICE_LOCALHOST , keytab_alice));
    jaas.append(registrySecurity.createJAASEntry(BOB_CLIENT_CONTEXT,
        BOB_LOCALHOST, keytab_bob));

    jaasFile = new File(kdcWorkDir, "jaas.txt");
    FileUtils.write(jaasFile, jaas.toString(), Charset.defaultCharset());
    LOG.info("\n"+ jaas);
    RegistrySecurity.bindJVMtoJAASFile(jaasFile);
  }


  //
  protected static final String kerberosRule =
      "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT";

  /**
   * Init hadoop security by setting up the UGI config
   */
  public static void initHadoopSecurity() {

    UserGroupInformation.setConfiguration(CONF);

    KerberosName.setRules(kerberosRule);
  }

  /**
   * Stop the secure ZK and log out the ZK account
   */
  public synchronized void stopSecureZK() {
    ServiceOperations.stop(secureZK);
    secureZK = null;
    logout(zookeeperLogin);
    zookeeperLogin = null;
  }


  public static MiniKdc getKdc() {
    return kdc;
  }

  public static File getKdcWorkDir() {
    return kdcWorkDir;
  }

  public static Properties getKdcConf() {
    return kdcConf;
  }

  /**
   * Create a secure instance
   * @param name instance name
   * @return the instance
   * @throws Exception
   */
  protected static MicroZookeeperService createSecureZKInstance(String name)
      throws Exception {
    String context = ZOOKEEPER_SERVER_CONTEXT;
    Configuration conf = new Configuration();

    File testdir = new File(System.getProperty("test.dir", "target"));
    File workDir = new File(testdir, name);
    if (!workDir.mkdirs()) {
      assertTrue(workDir.isDirectory());
    }
    System.setProperty(
        ZookeeperConfigOptions.PROP_ZK_SERVER_MAINTAIN_CONNECTION_DESPITE_SASL_FAILURE,
        "false");
    RegistrySecurity.validateContext(context);
    conf.set(MicroZookeeperServiceKeys.KEY_REGISTRY_ZKSERVICE_JAAS_CONTEXT,
        context);
    MicroZookeeperService secureZK = new MicroZookeeperService(name);
    secureZK.init(conf);
    LOG.info(secureZK.getDiagnostics());
    return secureZK;
  }

  /**
   * Create the keytabl for the given principal, includes
   * raw principal and $principal/localhost
   * @param principal principal short name
   * @param filename filename of keytab
   * @return file of keytab
   * @throws Exception
   */
  public static File createKeytab(String principal,
      String filename) throws Exception {
    assertNotEmpty("empty principal", principal);
    assertNotEmpty("empty host", filename);
    assertNotNull("Null KDC", kdc);
    File keytab = new File(kdcWorkDir, filename);
    kdc.createPrincipal(keytab,
        principal,
        principal + "/localhost",
        principal + "/127.0.0.1");
    return keytab;
  }

  public static String getPrincipalAndRealm(String principal) {
    return principal + "@" + getRealm();
  }

  protected static String getRealm() {
    return kdc.getRealm();
  }


  /**
   * Log in, defaulting to the client context
   * @param principal principal
   * @param context context
   * @param keytab keytab
   * @return the logged in context
   * @throws LoginException failure to log in
   * @throws FileNotFoundException no keytab
   */
  protected LoginContext login(String principal,
      String context, File keytab) throws LoginException,
      FileNotFoundException {
    LOG.info("Logging in as {} in context {} with keytab {}",
        principal, context, keytab);
    if (!keytab.exists()) {
      throw new FileNotFoundException(keytab.getAbsolutePath());
    }
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(principal));
    Subject subject = new Subject(false, principals, new HashSet<Object>(),
        new HashSet<Object>());
    LoginContext login;
    login = new LoginContext(context, subject, null,
        KerberosConfiguration.createClientConfig(principal, keytab));
    login.login();
    return login;
  }


  /**
   * Start the secure ZK instance using the test method name as the path.
   * As the entry is saved to the {@link #secureZK} field, it
   * is automatically stopped after the test case.
   * @throws Exception on any failure
   */
  protected synchronized void startSecureZK() throws Exception {
    assertNull("Zookeeper is already running", secureZK);

    zookeeperLogin = login(zkServerPrincipal,
        ZOOKEEPER_SERVER_CONTEXT,
        keytab_zk);
    secureZK = createSecureZKInstance("test-" + methodName.getMethodName());
    secureZK.start();
  }

}
