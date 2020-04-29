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

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.data.ACL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.registry.client.impl.zk.RegistrySecurity;
import org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions;

import static org.apache.hadoop.security.authentication.util.KerberosName.DEFAULT_MECHANISM;
import static org.apache.hadoop.security.authentication.util.KerberosName.MECHANISM_HADOOP;
import static org.apache.hadoop.security.authentication.util.KerberosName.MECHANISM_MIT;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that logins work
 */
public class TestSecureLogins extends AbstractSecureRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureLogins.class);

  @Test
  public void testHasRealm() throws Throwable {
    assertNotNull(getRealm());
    LOG.info("ZK principal = {}", getPrincipalAndRealm(ZOOKEEPER_LOCALHOST));
  }

  @Test
  public void testJaasFileSetup() throws Throwable {
    // the JVM has seemed inconsistent on setting up here
    assertNotNull("jaasFile", jaasFile);
    String confFilename = System.getProperty(Environment.JAAS_CONF_KEY);
    assertEquals(jaasFile.getAbsolutePath(), confFilename);
  }

  @Test
  public void testJaasFileBinding() throws Throwable {
    // the JVM has seemed inconsistent on setting up here
    assertNotNull("jaasFile", jaasFile);
    RegistrySecurity.bindJVMtoJAASFile(jaasFile);
    String confFilename = System.getProperty(Environment.JAAS_CONF_KEY);
    assertEquals(jaasFile.getAbsolutePath(), confFilename);
  }

  @Test
  public void testClientLogin() throws Throwable {
    LoginContext client = login(ALICE_LOCALHOST,
                                ALICE_CLIENT_CONTEXT,
                                keytab_alice);

    try {
      logLoginDetails(ALICE_LOCALHOST, client);
      String confFilename = System.getProperty(Environment.JAAS_CONF_KEY);
      assertNotNull("Unset: "+ Environment.JAAS_CONF_KEY, confFilename);
      String config = FileUtils.readFileToString(new File(confFilename),
          Charset.defaultCharset());
      LOG.info("{}=\n{}", confFilename, config);
      RegistrySecurity.setZKSaslClientProperties(ALICE, ALICE_CLIENT_CONTEXT);
    } finally {
      client.logout();
    }
  }

  @Test
  public void testZKServerContextLogin() throws Throwable {
    LoginContext client = login(ZOOKEEPER_LOCALHOST,
                                ZOOKEEPER_SERVER_CONTEXT,
                                keytab_zk);
    logLoginDetails(ZOOKEEPER_LOCALHOST, client);

    client.logout();
  }

  @Test
  public void testServerLogin() throws Throwable {
    LoginContext loginContext = createLoginContextZookeeperLocalhost();
    loginContext.login();
    loginContext.logout();
  }

  public LoginContext createLoginContextZookeeperLocalhost() throws
      LoginException {
    String principalAndRealm = getPrincipalAndRealm(ZOOKEEPER_LOCALHOST);
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(ZOOKEEPER_LOCALHOST));
    Subject subject = new Subject(false, principals, new HashSet<Object>(),
        new HashSet<Object>());
    return new LoginContext("", subject, null,
        KerberosConfiguration.createServerConfig(ZOOKEEPER_LOCALHOST, keytab_zk));
  }

  @Test
  public void testKerberosAuth() throws Throwable {
    File krb5conf = getKdc().getKrb5conf();
    String krbConfig = FileUtils.readFileToString(krb5conf,
        Charset.defaultCharset());
    LOG.info("krb5.conf at {}:\n{}", krb5conf, krbConfig);
    Subject subject = new Subject();
    Class<?> kerb5LoginClass =
        Class.forName(KerberosUtil.getKrb5LoginModuleName());
    Constructor<?> kerb5LoginConstr = kerb5LoginClass.getConstructor();
    Object kerb5LoginObject = kerb5LoginConstr.newInstance();
    final Map<String, String> options = new HashMap<String, String>();
    options.put("debug", "true");
    if (IBM_JAVA) {
      options.put("useKeytab",
          keytab_alice.getAbsolutePath().startsWith("file://")
            ? keytab_alice.getAbsolutePath()
            : "file://" +  keytab_alice.getAbsolutePath());
      options.put("principal", ALICE_LOCALHOST);
      options.put("refreshKrb5Config", "true");
      options.put("credsType", "both");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        // IBM JAVA only respect system property and not env variable
        // The first value searched when "useDefaultCcache" is used.
        System.setProperty("KRB5CCNAME", ticketCache);
        options.put("useDefaultCcache", "true");
        options.put("renewTGT", "true");
      }
    } else {
      options.put("keyTab", keytab_alice.getAbsolutePath());
      options.put("principal", ALICE_LOCALHOST);
      options.put("doNotPrompt", "true");
      options.put("isInitiator", "true");
      options.put("refreshKrb5Config", "true");
      options.put("renewTGT", "true");
      options.put("storeKey", "true");
      options.put("useKeyTab", "true");
      options.put("useTicketCache", "true");
    }
    Method methodInitialize =
        kerb5LoginObject.getClass().getMethod("initialize", Subject.class,
          CallbackHandler.class, Map.class, Map.class);
    methodInitialize.invoke(kerb5LoginObject, subject, null,
        new HashMap<String, String>(), options);
    Method methodLogin = kerb5LoginObject.getClass().getMethod("login");
    boolean loginOk = (Boolean) methodLogin.invoke(kerb5LoginObject);
    assertTrue("Failed to login", loginOk);
    Method methodCommit = kerb5LoginObject.getClass().getMethod("commit");
    boolean commitOk = (Boolean) methodCommit.invoke(kerb5LoginObject);
    assertTrue("Failed to Commit", commitOk);
  }

  @Test
  public void testDefaultRealmValid() throws Throwable {
    String defaultRealm = KerberosUtil.getDefaultRealm();
    assertNotEmpty("No default Kerberos Realm",
        defaultRealm);
    LOG.info("Default Realm '{}'", defaultRealm);
  }

  @Test
  public void testKerberosRulesValid() throws Throwable {
    assertTrue("!KerberosName.hasRulesBeenSet()",
        KerberosName.hasRulesBeenSet());
    String rules = KerberosName.getRules();
    assertEquals(kerberosRule, rules);
    LOG.info(rules);
  }

  @Test
  public void testValidKerberosName() throws Throwable {
    KerberosName.setRuleMechanism(MECHANISM_HADOOP);
    new HadoopKerberosName(ZOOKEEPER).getShortName();
    // MECHANISM_MIT allows '/' and '@' in username
    KerberosName.setRuleMechanism(MECHANISM_MIT);
    new HadoopKerberosName(ZOOKEEPER).getShortName();
    new HadoopKerberosName(ZOOKEEPER_LOCALHOST).getShortName();
    new HadoopKerberosName(ZOOKEEPER_REALM).getShortName();
    new HadoopKerberosName(ZOOKEEPER_LOCALHOST_REALM).getShortName();
    KerberosName.setRuleMechanism(DEFAULT_MECHANISM);
  }

  @Test
  public void testUGILogin() throws Throwable {

    UserGroupInformation ugi = loginUGI(ZOOKEEPER, keytab_zk);
    RegistrySecurity.UgiInfo ugiInfo =
        new RegistrySecurity.UgiInfo(ugi);
    LOG.info("logged in as: {}", ugiInfo);
    assertTrue("security is not enabled: " + ugiInfo,
        UserGroupInformation.isSecurityEnabled());
    assertTrue("login is keytab based: " + ugiInfo,
        ugi.isFromKeytab());

    // now we are here, build a SASL ACL
    ACL acl = ugi.doAs(new PrivilegedExceptionAction<ACL>() {
      @Override
      public ACL run() throws Exception {
        return registrySecurity.createSaslACLFromCurrentUser(0);
      }
    });
    assertEquals(ZOOKEEPER_REALM, acl.getId().getId());
    assertEquals(ZookeeperConfigOptions.SCHEME_SASL, acl.getId().getScheme());
    registrySecurity.addSystemACL(acl);

  }

}
