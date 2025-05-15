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
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.event.Level;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;

/**
 * Verify UGI login from keytab. Check that the UGI is
 * configured to use keytab to catch regressions like
 * HADOOP-10786.
 */
public class TestUGILoginFromKeytab {

  private MiniKdc kdc;
  private File workDir;
  private ExecutorService executor;

  @BeforeEach
  public void startMiniKdc(@TempDir Path tempDir) throws Exception {
    // This setting below is required. If not enabled, UGI will abort
    // any attempt to loginUserFromKeytab.
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    workDir = tempDir.toFile();
    kdc = new MiniKdc(MiniKdc.createConf(), workDir);
    kdc.start();
    executor = Executors.newCachedThreadPool();

  }

  @AfterEach
  public void stopMiniKdc() {
    if (kdc != null) {
      kdc.stop();
    }
    if (executor != null) {
      executor.shutdownNow();
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
    assertTrue(ugi.isFromKeytab(),
        "UGI should be configured to login from keytab");

    User user = getUser(ugi.getSubject());
    assertNotNull(user.getLogin());
 
    assertTrue(user.getLastLogin() > beforeLogin,
        "User login time is less than before login time, "
        + "beforeLoginTime:" + beforeLogin + " userLoginTime:" + user.getLastLogin());
  }

  /**
   * Login from keytab using the MiniKDC and verify the UGI can successfully
   * relogin from keytab as well. This will catch regressions like HADOOP-10786.
   */
  @Test
  public void testUGIReLoginFromKeytab() throws Exception {
    String principal = "foo";
    File keytab = new File(workDir, "foo.keytab");
    kdc.createPrincipal(keytab, principal);

    UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    assertTrue(ugi.isFromKeytab(), "UGI should be configured to login from keytab");

    // Verify relogin from keytab.
    User user = getUser(ugi.getSubject());
    final long firstLogin = user.getLastLogin();
    final LoginContext login1 = user.getLogin();
    assertNotNull(login1);

    // Sleep for 2 secs to have a difference between first and second login
    Thread.sleep(2000);

    ugi.reloginFromKeytab();
    final long secondLogin = user.getLastLogin();
    final LoginContext login2 = user.getLogin();
    assertTrue(secondLogin > firstLogin,
        "User should have been able to relogin from keytab");
    assertNotNull(login2);
    assertNotSame(login1, login2);
  }

  /**
   * Force re-login from keytab using the MiniKDC and verify the UGI can
   * successfully relogin from keytab as well.
   */
  @Test
  public void testUGIForceReLoginFromKeytab() throws Exception {
    // Set this to false as we are testing force re-login anyways
    UserGroupInformation.setShouldRenewImmediatelyForTests(false);
    String principal = "foo";
    File keytab = new File(workDir, "foo.keytab");
    kdc.createPrincipal(keytab, principal);

    UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    assertTrue(ugi.isFromKeytab(), "UGI should be configured to login from keytab");

    // Verify relogin from keytab.
    User user = getUser(ugi.getSubject());
    final long firstLogin = user.getLastLogin();
    final LoginContext login1 = user.getLogin();
    assertNotNull(login1);

    // Sleep for 2 secs to have a difference between first and second login
    Thread.sleep(2000);

    // Force relogin from keytab
    ugi.forceReloginFromKeytab();
    final long secondLogin = user.getLastLogin();
    final LoginContext login2 = user.getLogin();
    assertTrue(secondLogin > firstLogin,
        "User should have been able to relogin from keytab");
    assertNotNull(login2);
    assertNotSame(login1, login2);
  }

  @Test
  public void testGetUGIFromKnownSubject() throws Exception {
    KerberosPrincipal principal = new KerberosPrincipal("user");
    File keytab = new File(workDir, "user.keytab");
    kdc.createPrincipal(keytab, principal.getName());

    UserGroupInformation ugi1 =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        principal.getName(), keytab.getPath());
    Subject subject = ugi1.getSubject();
    User user = getUser(subject);
    assertNotNull(user);
    LoginContext login = user.getLogin();
    assertNotNull(login);

    // User instance and/or login context should not change.
    UserGroupInformation ugi2 = UserGroupInformation.getUGIFromSubject(subject);
    assertSame(user, getUser(ugi2.getSubject()));
    assertSame(login, user.getLogin());
  }

  @Test
  public void testGetUGIFromExternalSubject() throws Exception {
    KerberosPrincipal principal = new KerberosPrincipal("user");
    File keytab = new File(workDir, "user.keytab");
    kdc.createPrincipal(keytab, principal.getName());

    UserGroupInformation ugi =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        principal.getName(), keytab.getPath());
    Subject subject = ugi.getSubject();
    removeUser(subject);

    // first call to get the ugi should add the User instance w/o a login
    // context.
    UserGroupInformation ugi1 = UserGroupInformation.getUGIFromSubject(subject);
    assertSame(subject, ugi1.getSubject());
    User user = getUser(subject);
    assertNotNull(user);
    assertEquals(principal.getName(), user.getName());
    assertNull(user.getLogin());

    // subsequent call should not change the existing User instance.
    UserGroupInformation ugi2 = UserGroupInformation.getUGIFromSubject(subject);
    assertSame(subject, ugi2.getSubject());
    assertSame(user, getUser(ugi2.getSubject()));
    assertNull(user.getLogin());
  }

  @Test
  public void testGetUGIFromExternalSubjectWithLogin() throws Exception {
    KerberosPrincipal principal = new KerberosPrincipal("user");
    File keytab = new File(workDir, "user.keytab");
    kdc.createPrincipal(keytab, principal.getName());

    // alter the User's login context to appear to be a foreign and
    // unmanagable context.
    UserGroupInformation ugi =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        principal.getName(), keytab.getPath());
    Subject subject = ugi.getSubject();
    User user = getUser(subject);
    final LoginContext dummyLogin = Mockito.mock(LoginContext.class);
    user.setLogin(dummyLogin);

    // nothing should change.
    UserGroupInformation ugi2 = UserGroupInformation.getUGIFromSubject(subject);
    assertSame(subject, ugi2.getSubject());
    assertSame(user, getUser(ugi2.getSubject()));
    assertSame(dummyLogin, user.getLogin());
  }

  @Test
  public void testUGIRefreshFromKeytab() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED, true);
    SecurityUtil.setAuthenticationMethod(
            UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    String principal = "bar";
    File keytab = new File(workDir, "bar.keytab");
    kdc.createPrincipal(keytab, principal);

    UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();

    assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS,
        ugi.getAuthenticationMethod());
    assertTrue(ugi.isFromKeytab());
    assertTrue(UserGroupInformation.isKerberosKeyTabLoginRenewalEnabled());
    assertTrue(UserGroupInformation.getKerberosLoginRenewalExecutor()
        .isPresent());
  }

  @Test
  public void testUGIRefreshFromKeytabDisabled() throws Exception {
    GenericTestUtils.setLogLevel(UserGroupInformation.LOG, Level.DEBUG);
    final Configuration conf = new Configuration();
    conf.setLong(HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 1);
    conf.setBoolean(HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED, false);
    SecurityUtil.setAuthenticationMethod(
            UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    String principal = "bar";
    File keytab = new File(workDir, "bar.keytab");
    kdc.createPrincipal(keytab, principal);

    UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS,
        ugi.getAuthenticationMethod());
    assertTrue(ugi.isFromKeytab());
    assertFalse(UserGroupInformation.isKerberosKeyTabLoginRenewalEnabled());
    assertFalse(UserGroupInformation.getKerberosLoginRenewalExecutor()
        .isPresent());
  }

  private static KerberosTicket getTicket(UserGroupInformation ugi) {
    Set<KerberosTicket> tickets =
        ugi.getSubject().getPrivateCredentials(KerberosTicket.class);
    return tickets.isEmpty() ? null : tickets.iterator().next();
  }

  // verify ugi has expected principal, a keytab, and has a ticket for
  // the expected principal.
  private static KerberosTicket checkTicketAndKeytab(UserGroupInformation ugi,
      KerberosPrincipal principal, boolean expectIsKeytab) {
    assertEquals(principal.getName(), ugi.getUserName(), "wrong principal");
    assertEquals(expectIsKeytab, ugi.isFromKeytab(), "is not keytab");
    KerberosTicket ticket = getTicket(ugi);
    assertNotNull(ticket, "no ticket");
    assertEquals(principal, ticket.getClient(), "wrong principal");
    return ticket;
  }

  @Test
  public void testReloginForUGIFromSubject() throws Exception {
    KerberosPrincipal principal1 = new KerberosPrincipal("user1");
    File keytab1 = new File(workDir, "user1.keytab");
    kdc.createPrincipal(keytab1, principal1.getName());

    KerberosPrincipal principal2 = new KerberosPrincipal("user2");
    File keytab2 = new File(workDir, "user2.keytab");
    kdc.createPrincipal(keytab2, principal2.getName());

    // Login a user and remove the User instance so it looks like an
    // "external" subject.
    final Subject extSubject =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        principal2.getName(), keytab2.getPath()).getSubject();
    removeUser(extSubject);

    // Login another user.
    UserGroupInformation.loginUserFromKeytab(
        principal1.getName(), keytab1.getPath());
    final UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

    loginUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws IOException {
        KerberosTicket loginTicket =
            checkTicketAndKeytab(loginUser, principal1, true);

        // get the ugi for the previously logged in subject.
        UserGroupInformation extSubjectUser =
            UserGroupInformation.getUGIFromSubject(extSubject);
        KerberosTicket ticket =
          checkTicketAndKeytab(extSubjectUser, principal2, false);

        // verify login user got a new ticket.
        loginUser.reloginFromKeytab();
        KerberosTicket newLoginTicket =
            checkTicketAndKeytab(loginUser, principal1, true);
        assertNotEquals(loginTicket.getAuthTime(),
            newLoginTicket.getAuthTime());

        // verify an "external" subject ticket does not change.
        extSubjectUser.reloginFromKeytab();
        assertSame(ticket,
            checkTicketAndKeytab(extSubjectUser, principal2, false));

        // verify subject ugi relogin did not affect the login user.
        assertSame(newLoginTicket,
            checkTicketAndKeytab(loginUser, principal1, true));

        return null;
      }
    });
  }

  @Test
  public void testReloginForLoginFromSubject() throws Exception {
    KerberosPrincipal principal1 = new KerberosPrincipal("user1");
    File keytab1 = new File(workDir, "user1.keytab");
    kdc.createPrincipal(keytab1, principal1.getName());

    KerberosPrincipal principal2 = new KerberosPrincipal("user2");
    File keytab2 = new File(workDir, "user2.keytab");
    kdc.createPrincipal(keytab2, principal2.getName());

    // login principal1 with a keytab.
    UserGroupInformation.loginUserFromKeytab(
        principal1.getName(), keytab1.getPath());
    final UserGroupInformation originalLoginUser =
        UserGroupInformation.getLoginUser();
    assertNotNull(getUser(originalLoginUser.getSubject()).getLogin());

    originalLoginUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws IOException {
        KerberosTicket originalLoginUserTicket =
            checkTicketAndKeytab(originalLoginUser, principal1, true);

        // login principal2 from a subject with keytab.  it's external so
        // no login context should be attached to the user.
        final Subject subject =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            principal2.getName(), keytab2.getPath()).getSubject();
        removeUser(subject);

        // verify the new login user is external.
        UserGroupInformation.loginUserFromSubject(subject);
        assertNull(getUser(subject).getLogin());
        UserGroupInformation extLoginUser =
          UserGroupInformation.getLoginUser();
        KerberosTicket extLoginUserTicket =
            checkTicketAndKeytab(extLoginUser, principal2, false);

        // verify subject-based login user does not get a new ticket, and
        // original login user not affected.
        extLoginUser.reloginFromKeytab();
        assertSame(extLoginUserTicket,
            checkTicketAndKeytab(extLoginUser, principal2, false));
        assertSame(originalLoginUserTicket,
            checkTicketAndKeytab(originalLoginUser, principal1, true));

        // verify original login user gets a new ticket, new login user
        // not affected.
        originalLoginUser.reloginFromKeytab();
        assertNotSame(originalLoginUserTicket,
            checkTicketAndKeytab(originalLoginUser, principal1, true));
        assertSame(extLoginUserTicket,
            checkTicketAndKeytab(extLoginUser, principal2, false));
        return null;
      }
    });
  }

  @Test
  public void testReloginAfterFailedRelogin() throws Exception {
    KerberosPrincipal principal = new KerberosPrincipal("user1");
    File keytab = new File(workDir, "user1.keytab");
    File keytabBackup = new File(keytab + ".backup");
    kdc.createPrincipal(keytab, principal.getName());

    UserGroupInformation.loginUserFromKeytab(
        principal.getName(), keytab.getPath());
    final UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    checkTicketAndKeytab(loginUser, principal, true);

    // move the keytab to induce a relogin failure.
    assertTrue(keytab.renameTo(keytabBackup));
    try {
      loginUser.reloginFromKeytab();
      fail("relogin should fail");
    } catch (KerberosAuthException kae) {
      // expected.
    }

    // even though no KeyTab object, ugi should know it's keytab based.
    assertTrue(loginUser.isFromKeytab());
    assertNull(getTicket(loginUser));

    // move keytab back to enable relogin to succeed.
    assertTrue(keytabBackup.renameTo(keytab));
    loginUser.reloginFromKeytab();
    checkTicketAndKeytab(loginUser, principal, true);
  }

  // verify getting concurrent relogins blocks to avoid indeterminate
  // credentials corruption, but getting a ugi for the subject does not block.
  @Test
  @Timeout(value = 180)
  public void testConcurrentRelogin() throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final CountDownLatch latch = new CountDownLatch(1);
    assertTrue(UserGroupInformation.isSecurityEnabled());

    KerberosPrincipal principal = new KerberosPrincipal("testUser");
    File keytab = new File(workDir, "user1.keytab");
    kdc.createPrincipal(keytab, principal.getName());

    // create a keytab ugi.
    final UserGroupInformation loginUgi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          principal.getName(), keytab.getPath());
    assertEquals(AuthenticationMethod.KERBEROS,
        loginUgi.getAuthenticationMethod());
    assertTrue(loginUgi.isFromKeytab());

    // create a new ugi instance based on subject from the logged in user.
    final UserGroupInformation clonedUgi =
        UserGroupInformation.getUGIFromSubject(loginUgi.getSubject());
    assertEquals(AuthenticationMethod.KERBEROS,
        clonedUgi.getAuthenticationMethod());
    assertTrue(clonedUgi.isFromKeytab());

    // cause first relogin to block on a barrier in logout to verify relogins
    // are atomic.
    User user = getUser(loginUgi.getSubject());
    final LoginContext spyLogin = Mockito.spy(user.getLogin());
    user.setLogin(spyLogin);
    Mockito.doAnswer(new Answer<Void>(){
      @Override
      public Void answer(InvocationOnMock invocation)
          throws Throwable {
        invocation.callRealMethod();
        latch.countDown();
        barrier.await();
        return null;
      }
    }).when(spyLogin).logout();

    Future<Void> relogin = executor.submit(
        new Callable<Void>(){
          @Override
          public Void call() throws Exception {
            Thread.currentThread().setName("relogin");
            loginUgi.reloginFromKeytab();
            return null;
          }
        });
    // wait for the thread to block on the barrier in the logout of the
    // relogin.
    assertTrue(latch.await(2, TimeUnit.SECONDS), "first relogin didn't block");

    // although the logout removed the keytab instance, verify the ugi
    // knows from its login params that it is supposed to be from a keytab.
    assertTrue(clonedUgi.isFromKeytab());

    // another concurrent re-login should block.
    Mockito.doNothing().when(spyLogin).logout();
    Mockito.doNothing().when(spyLogin).login();
    Future<UserGroupInformation> clonedRelogin = executor.submit(
        new Callable<UserGroupInformation>(){
          @Override
          public UserGroupInformation call() throws Exception {
            Thread.currentThread().setName("clonedRelogin");
            clonedUgi.reloginFromKeytab();
            return clonedUgi;
          }
        });

    try {
      clonedRelogin.get(2, TimeUnit.SECONDS);
      fail("second relogin didn't block!");
    } catch (TimeoutException te) {
      // expected
    }

    // concurrent UGI instantiation should not block and again should
    // know it's supposed to be from a keytab.
    loginUgi.doAs(new PrivilegedExceptionAction<Void>(){
      @Override
      public Void run() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        assertEquals(principal.getName(), ugi.getUserName());
        assertTrue(ugi.isFromKeytab());
        return null;
      }
    });
    clonedUgi.doAs(new PrivilegedExceptionAction<Void>(){
      @Override
      public Void run() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        assertEquals(principal.getName(), ugi.getUserName());
        assertTrue(ugi.isFromKeytab());
        return null;
      }
    });

    // second relogin should still be blocked until the original relogin
    // is blocked.
    assertFalse(clonedRelogin.isDone());
    barrier.await();
    relogin.get();
    clonedRelogin.get();
  }

  private User getUser(Subject subject) {
    Iterator<User> iter = subject.getPrincipals(User.class).iterator();
    return iter.hasNext() ? iter.next() : null;
  }

  private void removeUser(Subject subject) {
    // remove User instance so it appears to not be logged in.
    for (Iterator<Principal> iter = subject.getPrincipals().iterator();
         iter.hasNext(); ) {
      if (iter.next() instanceof User) {
        iter.remove();
      }
    }
  }
}
