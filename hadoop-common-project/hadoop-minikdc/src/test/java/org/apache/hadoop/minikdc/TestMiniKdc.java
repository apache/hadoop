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

package org.apache.hadoop.minikdc;

import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.io.File;
import java.security.Principal;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Arrays;

public class TestMiniKdc extends KerberosSecurityTestcase {

  @Test
  public void testMiniKdcStart() {
    MiniKdc kdc = getKdc();
    Assert.assertNotSame(0, kdc.getPort());
  }

  @Test
  public void testKeytabGen() throws Exception {
    MiniKdc kdc = getKdc();
    File workDir = getWorkDir();

    kdc.createPrincipal(new File(workDir, "keytab"), "foo/bar", "bar/foo");
    Keytab kt = Keytab.read(new File(workDir, "keytab"));
    Set<String> principals = new HashSet<String>();
    for (KeytabEntry entry : kt.getEntries()) {
      principals.add(entry.getPrincipalName());
    }
    //here principals use \ instead of /
    //because org.apache.directory.server.kerberos.shared.keytab.KeytabDecoder
    // .getPrincipalName(IoBuffer buffer) use \\ when generates principal
    Assert.assertEquals(new HashSet<String>(Arrays.asList(
            "foo\\bar@" + kdc.getRealm(), "bar\\foo@" + kdc.getRealm())),
            principals);
  }

  private static class KerberosConfiguration extends Configuration {
    private String principal;
    private String keytab;
    private boolean isInitiator;

    private KerberosConfiguration(String principal, File keytab,
                                  boolean client) {
      this.principal = principal;
      this.keytab = keytab.getAbsolutePath();
      this.isInitiator = client;
    }

    public static Configuration createClientConfig(String principal,
                                                   File keytab) {
      return new KerberosConfiguration(principal, keytab, true);
    }

    public static Configuration createServerConfig(String principal,
                                                   File keytab) {
      return new KerberosConfiguration(principal, keytab, false);
    }

    private static String getKrb5LoginModuleName() {
      return System.getProperty("java.vendor").contains("IBM")
              ? "com.ibm.security.auth.module.Krb5LoginModule"
              : "com.sun.security.auth.module.Krb5LoginModule";
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("keyTab", keytab);
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", Boolean.toString(isInitiator));
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[]{
              new AppConfigurationEntry(getKrb5LoginModuleName(),
                      AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                      options)};
    }
  }

  @Test
  public void testKerberosLogin() throws Exception {
    MiniKdc kdc = getKdc();
    File workDir = getWorkDir();
    LoginContext loginContext = null;
    try {
      String principal = "foo";
      File keytab = new File(workDir, "foo.keytab");
      kdc.createPrincipal(keytab, principal);

      Set<Principal> principals = new HashSet<Principal>();
      principals.add(new KerberosPrincipal(principal));

      //client login
      Subject subject = new Subject(false, principals, new HashSet<Object>(),
              new HashSet<Object>());
      loginContext = new LoginContext("", subject, null,
              KerberosConfiguration.createClientConfig(principal, keytab));
      loginContext.login();
      subject = loginContext.getSubject();
      Assert.assertEquals(1, subject.getPrincipals().size());
      Assert.assertEquals(KerberosPrincipal.class,
              subject.getPrincipals().iterator().next().getClass());
      Assert.assertEquals(principal + "@" + kdc.getRealm(),
              subject.getPrincipals().iterator().next().getName());
      loginContext.logout();

      //server login
      subject = new Subject(false, principals, new HashSet<Object>(),
              new HashSet<Object>());
      loginContext = new LoginContext("", subject, null,
              KerberosConfiguration.createServerConfig(principal, keytab));
      loginContext.login();
      subject = loginContext.getSubject();
      Assert.assertEquals(1, subject.getPrincipals().size());
      Assert.assertEquals(KerberosPrincipal.class,
              subject.getPrincipals().iterator().next().getClass());
      Assert.assertEquals(principal + "@" + kdc.getRealm(),
              subject.getPrincipals().iterator().next().getName());
      loginContext.logout();

    } finally {
      if (loginContext != null) {
        loginContext.logout();
      }
    }
  }

}
