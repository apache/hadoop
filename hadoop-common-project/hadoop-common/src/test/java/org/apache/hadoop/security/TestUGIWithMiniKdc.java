/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.PlatformName;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Test;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import java.io.File;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN;

/**
 * Test {@link UserGroupInformation} with a minikdc.
 */
public class TestUGIWithMiniKdc {

  private static MiniKdc kdc;

  @After
  public void teardown() {
    UserGroupInformation.reset();
    if (kdc != null) {
      kdc.stop();
    }
  }

  private void setupKdc() throws Exception {
    Properties kdcConf = MiniKdc.createConf();
    // tgt expire time = 30 seconds
    kdcConf.setProperty(MiniKdc.MAX_TICKET_LIFETIME, "30");
    kdcConf.setProperty(MiniKdc.MIN_TICKET_LIFETIME, "30");
    File kdcDir = new File(System.getProperty("test.dir", "target"));
    kdc = new MiniKdc(kdcConf, kdcDir);
    kdc.start();
  }

  @Test(timeout = 120000)
  public void testAutoRenewalThreadRetryWithKdc() throws Exception {
    GenericTestUtils.setLogLevel(UserGroupInformation.LOG, Level.DEBUG);
    final Configuration conf = new Configuration();
    // Relogin every 1 second
    conf.setLong(HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 1);
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    LoginContext loginContext = null;
    try {
      final String principal = "foo";
      final File workDir = new File(System.getProperty("test.dir", "target"));
      final File keytab = new File(workDir, "foo.keytab");
      final Set<Principal> principals = new HashSet<>();
      principals.add(new KerberosPrincipal(principal));
      setupKdc();
      kdc.createPrincipal(keytab, principal);

      // client login
      final Subject subject =
          new Subject(false, principals, new HashSet<>(), new HashSet<>());

      loginContext = new LoginContext("", subject, null,
          new javax.security.auth.login.Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(
                String name) {
              Map<String, String> options = new HashMap<>();
              options.put("principal", principal);
              options.put("refreshKrb5Config", "true");
              if (PlatformName.IBM_JAVA) {
                options.put("useKeytab", keytab.getPath());
                options.put("credsType", "both");
              } else {
                options.put("keyTab", keytab.getPath());
                options.put("useKeyTab", "true");
                options.put("storeKey", "true");
                options.put("doNotPrompt", "true");
                options.put("useTicketCache", "true");
                options.put("renewTGT", "true");
                options.put("isInitiator", Boolean.toString(true));
              }
              String ticketCache = System.getenv("KRB5CCNAME");
              if (ticketCache != null) {
                options.put("ticketCache", ticketCache);
              }
              options.put("debug", "true");
              return new AppConfigurationEntry[] {new AppConfigurationEntry(
                  KerberosUtil.getKrb5LoginModuleName(),
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  options)};
            }
          });
      loginContext.login();
      final Subject loginSubject = loginContext.getSubject();
      UserGroupInformation.loginUserFromSubject(loginSubject);

      // Verify retry happens. Do not verify retry count to reduce flakiness.
      // Detailed back-off logic is tested separately in
      // TestUserGroupInformation#testGetNextRetryTime
      LambdaTestUtils.await(30000, 500,
          () -> {
            final int count =
                UserGroupInformation.metrics.getRenewalFailures().value();
            UserGroupInformation.LOG.info("Renew failure count is {}", count);
            return count > 0;
          });
    } finally {
      if (loginContext != null) {
        loginContext.logout();
      }
    }
  }
}
