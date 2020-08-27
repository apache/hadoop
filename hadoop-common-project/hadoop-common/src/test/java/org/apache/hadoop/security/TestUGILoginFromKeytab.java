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

package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import javax.security.auth.login.LoginContext;

/**
 * Verify UGI login from keytab. Check that the UGI is
 * configured to use keytab to catch regressions like
 * HADOOP-10786.
 */
public class TestUGILoginFromKeytab {

  private MiniKdc kdc;
  private File workDir;

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void startMiniKdc() throws Exception {
    // This setting below is required. If not enabled, UGI will abort
    // any attempt to loginUserFromKeytab.
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    workDir = folder.getRoot();
    kdc = new MiniKdc(MiniKdc.createConf(), workDir);
    kdc.start();
  }

  @After
  public void stopMiniKdc() {
    if (kdc != null) {
      kdc.stop();
    }
  }

  /**
   * Login from keytab using the MiniKDC.
   */
  @Test
  public void testUGILoginFromKeytab() throws Exception {
    long beforeLogin = Time.now();
    String principal = "foo";
    File keytab = new File(workDir, "foo.keytab");
    kdc.createPrincipal(keytab, principal);

    UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    Assert.assertTrue("UGI should be configured to login from keytab",
        ugi.isFromKeytab());

    User user = ugi.getSubject().getPrincipals(User.class).iterator().next();
    Assert.assertNotNull(user.getLogin());

    Assert.assertTrue(
        "User login time is less than before login time, " + "beforeLoginTime:"
            + beforeLogin + " userLoginTime:" + user.getLastLogin(),
        user.getLastLogin() > beforeLogin);
  }

  /**
   * Login from keytab using the MiniKDC and verify the UGI can successfully
   * relogin from keytab as well. This will catch regressions like HADOOP-10786.
   */
  @Test
  public void testUGIReloginFromKeytab() throws Exception {
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    String principal = "foo";
    File keytab = new File(workDir, "foo.keytab");
    kdc.createPrincipal(keytab, principal);

    UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    Assert.assertTrue("UGI should be configured to login from keytab",
        ugi.isFromKeytab());

    // Verify relogin from keytab.
    User user = ugi.getSubject().getPrincipals(User.class).iterator().next();
    final long firstLogin = user.getLastLogin();
    // Sleep for 2 secs to have a difference between first and second login
    Thread.sleep(2000);
    ugi.reloginFromKeytab();
    final long secondLogin = user.getLastLogin();
    Assert.assertTrue("User should have been able to relogin from keytab",
        secondLogin > firstLogin);
  }

  /**
   * Force re-login from keytab using the MiniKDC and verify the UGI can
   * successfully relogin from keytab as well.
   */
  @Test
  public void testUGIForceReLoginFromKeytab() throws Exception {
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    String principal = "foo";
    File keytab = new File(workDir, "foo.keytab");
    kdc.createPrincipal(keytab, principal);

    UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    Assert.assertTrue("UGI should be configured to login from keytab",
        ugi.isFromKeytab());

    // Verify relogin from keytab.
    User user = ugi.getSubject().getPrincipals(User.class).iterator().next();
    final long firstLogin = user.getLastLogin();
    final LoginContext login1 = user.getLogin();
    Assert.assertNotNull(login1);

    // Sleep for 2 secs to have a difference between first and second login
    Thread.sleep(2000);

    // Force relogin from keytab
    ugi.forceReloginFromKeytab();
    final long secondLogin = user.getLastLogin();
    final LoginContext login2 = user.getLogin();
    Assert.assertTrue("User should have been able to relogin from keytab",
        secondLogin > firstLogin);
    Assert.assertNotNull(login2);
    Assert.assertNotSame(login1, login2);
  }

}
