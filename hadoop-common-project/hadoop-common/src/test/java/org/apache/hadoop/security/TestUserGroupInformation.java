/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import static org.junit.Assert.*;
import org.junit.*;

import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import static org.apache.hadoop.test.MetricsAsserts.*;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

public class TestUserGroupInformation {
  final private static String USER_NAME = "user1@HADOOP.APACHE.ORG";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String[] GROUP_NAMES = 
    new String[]{GROUP1_NAME, GROUP2_NAME, GROUP3_NAME}; 
  private static Configuration conf;
 
  /**
   * UGI should not use the default security conf, else it will collide
   * with other classes that may change the default conf.  Using this dummy
   * class that simply throws an exception will ensure that the tests fail
   * if UGI uses the static default config instead of its own config
   */
  private static class DummyLoginConfiguration extends
    javax.security.auth.login.Configuration
  {
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      throw new RuntimeException("UGI is not using its own security conf!");
    } 
  }
  
  /** configure ugi */
  @BeforeClass
  public static void setup() {
    javax.security.auth.login.Configuration.setConfiguration(
        new DummyLoginConfiguration());
  }
  
  @Before
  public void setupUgi() {
    conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[2:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//" +
        "RULE:[1:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//"
        + "DEFAULT");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setLoginUser(null);
  }
  
  @After
  public void resetUgi() {
    UserGroupInformation.setLoginUser(null);
  }

  @Test
  public void testSimpleLogin() throws IOException {
    tryLoginAuthenticationMethod(AuthenticationMethod.SIMPLE, true);
  }

  @Test
  public void testTokenLogin() throws IOException {
    tryLoginAuthenticationMethod(AuthenticationMethod.TOKEN, false);
  }
  
  @Test
  public void testProxyLogin() throws IOException {
    tryLoginAuthenticationMethod(AuthenticationMethod.PROXY, false);
  }
  
  private void tryLoginAuthenticationMethod(AuthenticationMethod method,
                                            boolean expectSuccess)
                                                throws IOException {
    SecurityUtil.setAuthenticationMethod(method, conf);
    UserGroupInformation.setConfiguration(conf); // pick up changed auth       

    UserGroupInformation ugi = null;
    Exception ex = null;
    try {
      ugi = UserGroupInformation.getLoginUser();
    } catch (Exception e) {
      ex = e;
    }
    if (expectSuccess) {
      assertNotNull(ugi);
      assertEquals(method, ugi.getAuthenticationMethod());
    } else {
      assertNotNull(ex);
      assertEquals(UnsupportedOperationException.class, ex.getClass());
      assertEquals(method + " login authentication is not supported",
                   ex.getMessage());
    }
  }
  
  @Test
  public void testGetRealAuthenticationMethod() {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("user1");
    ugi.setAuthenticationMethod(AuthenticationMethod.SIMPLE);
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getRealAuthenticationMethod());
    ugi = UserGroupInformation.createProxyUser("user2", ugi);
    assertEquals(AuthenticationMethod.PROXY, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getRealAuthenticationMethod());
  }
  /** Test login method */
  @Test
  public void testLogin() throws Exception {
    // login from unix
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    assertEquals(UserGroupInformation.getCurrentUser(),
                 UserGroupInformation.getLoginUser());
    assertTrue(ugi.getGroupNames().length >= 1);

    // ensure that doAs works correctly
    UserGroupInformation userGroupInfo = 
      UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES);
    UserGroupInformation curUGI = 
      userGroupInfo.doAs(new PrivilegedExceptionAction<UserGroupInformation>(){
        @Override
        public UserGroupInformation run() throws IOException {
          return UserGroupInformation.getCurrentUser();
        }});
    // make sure in the scope of the doAs, the right user is current
    assertEquals(curUGI, userGroupInfo);
    // make sure it is not the same as the login user
    assertFalse(curUGI.equals(UserGroupInformation.getLoginUser()));
  }

  /**
   * given user name - get all the groups.
   * Needs to happen before creating the test users
   */
  @Test
  public void testGetServerSideGroups() throws IOException,
                                               InterruptedException {
    // get the user name
    Process pp = Runtime.getRuntime().exec("whoami");
    BufferedReader br = new BufferedReader
                          (new InputStreamReader(pp.getInputStream()));
    String userName = br.readLine().trim();
    // get the groups
    pp = Runtime.getRuntime().exec("id -Gn " + userName);
    br = new BufferedReader(new InputStreamReader(pp.getInputStream()));
    String line = br.readLine();
    System.out.println(userName + ":" + line);
   
    Set<String> groups = new LinkedHashSet<String> ();    
    for(String s: line.split("[\\s]")) {
      groups.add(s);
    }
    
    final UserGroupInformation login = UserGroupInformation.getCurrentUser();
    assertEquals(userName, login.getShortUserName());
    String[] gi = login.getGroupNames();
    assertEquals(groups.size(), gi.length);
    for(int i=0; i < gi.length; i++) {
      assertTrue(groups.contains(gi[i]));
    }
    
    final UserGroupInformation fakeUser = 
      UserGroupInformation.createRemoteUser("foo.bar");
    fakeUser.doAs(new PrivilegedExceptionAction<Object>(){
      @Override
      public Object run() throws IOException {
        UserGroupInformation current = UserGroupInformation.getCurrentUser();
        assertFalse(current.equals(login));
        assertEquals(current, fakeUser);
        assertEquals(0, current.getGroupNames().length);
        return null;
      }});
  }

  /** test constructor */
  @Test
  public void testConstructor() throws Exception {
    UserGroupInformation ugi = 
      UserGroupInformation.createUserForTesting("user2/cron@HADOOP.APACHE.ORG", 
                                                GROUP_NAMES);
    // make sure the short and full user names are correct
    assertEquals("user2/cron@HADOOP.APACHE.ORG", ugi.getUserName());
    assertEquals("user2", ugi.getShortUserName());
    ugi = UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES);
    assertEquals("user1", ugi.getShortUserName());
    
    // failure test
    testConstructorFailures(null);
    testConstructorFailures("");
  }

  private void testConstructorFailures(String userName) {
    boolean gotException = false;
    try {
      UserGroupInformation.createRemoteUser(userName);
    } catch (Exception e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  @Test
  public void testEquals() throws Exception {
    UserGroupInformation uugi = 
      UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES);

    assertEquals(uugi, uugi);
    // The subjects should be different, so this should fail
    UserGroupInformation ugi2 = 
      UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES);
    assertFalse(uugi.equals(ugi2));
    assertFalse(uugi.hashCode() == ugi2.hashCode());

    // two ugi that have the same subject need to be equal
    UserGroupInformation ugi3 = new UserGroupInformation(uugi.getSubject());
    assertEquals(uugi, ugi3);
    assertEquals(uugi.hashCode(), ugi3.hashCode());
  }
  
  @Test
  public void testEqualsWithRealUser() throws Exception {
    UserGroupInformation realUgi1 = UserGroupInformation.createUserForTesting(
        "RealUser", GROUP_NAMES);
    UserGroupInformation proxyUgi1 = UserGroupInformation.createProxyUser(
        USER_NAME, realUgi1);
    UserGroupInformation proxyUgi2 =
      new UserGroupInformation( proxyUgi1.getSubject());
    UserGroupInformation remoteUgi = UserGroupInformation.createRemoteUser(USER_NAME);
    assertEquals(proxyUgi1, proxyUgi2);
    assertFalse(remoteUgi.equals(proxyUgi1));
  }
  
  @Test
  public void testGettingGroups() throws Exception {
    UserGroupInformation uugi = 
      UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES);
    assertEquals(USER_NAME, uugi.getUserName());
    assertArrayEquals(new String[]{GROUP1_NAME, GROUP2_NAME, GROUP3_NAME},
                      uugi.getGroupNames());
  }

  @SuppressWarnings("unchecked") // from Mockito mocks
  @Test
  public <T extends TokenIdentifier> void testAddToken() throws Exception {
    UserGroupInformation ugi = 
        UserGroupInformation.createRemoteUser("someone"); 
    
    Token<T> t1 = mock(Token.class);
    Token<T> t2 = mock(Token.class);
    Token<T> t3 = mock(Token.class);
    
    // add token to ugi
    ugi.addToken(t1);
    checkTokens(ugi, t1);

    // replace token t1 with t2 - with same key (null)
    ugi.addToken(t2);
    checkTokens(ugi, t2);
    
    // change t1 service and add token
    when(t1.getService()).thenReturn(new Text("t1"));
    ugi.addToken(t1);
    checkTokens(ugi, t1, t2);
  
    // overwrite t1 token with t3 - same key (!null)
    when(t3.getService()).thenReturn(new Text("t1"));
    ugi.addToken(t3);
    checkTokens(ugi, t2, t3);

    // just try to re-add with new name
    when(t1.getService()).thenReturn(new Text("t1.1"));
    ugi.addToken(t1);
    checkTokens(ugi, t1, t2, t3);    

    // just try to re-add with new name again
    ugi.addToken(t1);
    checkTokens(ugi, t1, t2, t3);    
  }

  @SuppressWarnings("unchecked") // from Mockito mocks
  @Test
  public <T extends TokenIdentifier> void testGetCreds() throws Exception {
    UserGroupInformation ugi = 
        UserGroupInformation.createRemoteUser("someone"); 
    
    Text service = new Text("service");
    Token<T> t1 = mock(Token.class);
    when(t1.getService()).thenReturn(service);
    Token<T> t2 = mock(Token.class);
    when(t2.getService()).thenReturn(new Text("service2"));
    Token<T> t3 = mock(Token.class);
    when(t3.getService()).thenReturn(service);
    
    // add token to ugi
    ugi.addToken(t1);
    ugi.addToken(t2);
    checkTokens(ugi, t1, t2);

    Credentials creds = ugi.getCredentials();
    creds.addToken(t3.getService(), t3);
    assertSame(t3, creds.getToken(service));
    // check that ugi wasn't modified
    checkTokens(ugi, t1, t2);
  }

  @SuppressWarnings("unchecked") // from Mockito mocks
  @Test
  public <T extends TokenIdentifier> void testAddCreds() throws Exception {
    UserGroupInformation ugi = 
        UserGroupInformation.createRemoteUser("someone"); 
    
    Text service = new Text("service");
    Token<T> t1 = mock(Token.class);
    when(t1.getService()).thenReturn(service);
    Token<T> t2 = mock(Token.class);
    when(t2.getService()).thenReturn(new Text("service2"));
    byte[] secret = new byte[]{};
    Text secretKey = new Text("sshhh");

    // fill credentials
    Credentials creds = new Credentials();
    creds.addToken(t1.getService(), t1);
    creds.addToken(t2.getService(), t2);
    creds.addSecretKey(secretKey, secret);
    
    // add creds to ugi, and check ugi
    ugi.addCredentials(creds);
    checkTokens(ugi, t1, t2);
    assertSame(secret, ugi.getCredentials().getSecretKey(secretKey));
  }

  @Test
  public <T extends TokenIdentifier> void testGetCredsNotSame()
      throws Exception {
    UserGroupInformation ugi = 
        UserGroupInformation.createRemoteUser("someone"); 
    Credentials creds = ugi.getCredentials();
    // should always get a new copy
    assertNotSame(creds, ugi.getCredentials());
  }

  
  private void checkTokens(UserGroupInformation ugi, Token<?> ... tokens) {
    // check the ugi's token collection
    Collection<Token<?>> ugiTokens = ugi.getTokens();
    for (Token<?> t : tokens) {
      assertTrue(ugiTokens.contains(t));
    }
    assertEquals(tokens.length, ugiTokens.size());

    // check the ugi's credentials
    Credentials ugiCreds = ugi.getCredentials();
    for (Token<?> t : tokens) {
      assertSame(t, ugiCreds.getToken(t.getService()));
    }
    assertEquals(tokens.length, ugiCreds.numberOfTokens());
  }

  @SuppressWarnings("unchecked") // from Mockito mocks
  @Test
  public <T extends TokenIdentifier> void testAddNamedToken() throws Exception {
    UserGroupInformation ugi = 
        UserGroupInformation.createRemoteUser("someone"); 
    
    Token<T> t1 = mock(Token.class);
    Text service1 = new Text("t1");
    Text service2 = new Text("t2");
    when(t1.getService()).thenReturn(service1);
    
    // add token
    ugi.addToken(service1, t1);
    assertSame(t1, ugi.getCredentials().getToken(service1));

    // add token with another name
    ugi.addToken(service2, t1);
    assertSame(t1, ugi.getCredentials().getToken(service1));
    assertSame(t1, ugi.getCredentials().getToken(service2));
  }

  @SuppressWarnings("unchecked") // from Mockito mocks
  @Test
  public <T extends TokenIdentifier> void testUGITokens() throws Exception {
    UserGroupInformation ugi = 
      UserGroupInformation.createUserForTesting("TheDoctor", 
                                                new String [] { "TheTARDIS"});
    Token<T> t1 = mock(Token.class);
    when(t1.getService()).thenReturn(new Text("t1"));
    Token<T> t2 = mock(Token.class);
    when(t2.getService()).thenReturn(new Text("t2"));
    
    Credentials creds = new Credentials();
    byte[] secretKey = new byte[]{};
    Text secretName = new Text("shhh");
    creds.addSecretKey(secretName, secretKey);
    
    ugi.addToken(t1);
    ugi.addToken(t2);
    ugi.addCredentials(creds);
    
    Collection<Token<? extends TokenIdentifier>> z = ugi.getTokens();
    assertTrue(z.contains(t1));
    assertTrue(z.contains(t2));
    assertEquals(2, z.size());
    Credentials ugiCreds = ugi.getCredentials();
    assertSame(secretKey, ugiCreds.getSecretKey(secretName));
    assertEquals(1, ugiCreds.numberOfSecretKeys());
    
    try {
      z.remove(t1);
      fail("Shouldn't be able to modify token collection from UGI");
    } catch(UnsupportedOperationException uoe) {
      // Can't modify tokens
    }
    
    // ensure that the tokens are passed through doAs
    Collection<Token<? extends TokenIdentifier>> otherSet = 
      ugi.doAs(new PrivilegedExceptionAction<Collection<Token<?>>>(){
        @Override
        public Collection<Token<?>> run() throws IOException {
          return UserGroupInformation.getCurrentUser().getTokens();
        }
      });
    assertTrue(otherSet.contains(t1));
    assertTrue(otherSet.contains(t2));
  }
  
  @Test
  public void testTokenIdentifiers() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "TheDoctor", new String[] { "TheTARDIS" });
    TokenIdentifier t1 = mock(TokenIdentifier.class);
    TokenIdentifier t2 = mock(TokenIdentifier.class);

    ugi.addTokenIdentifier(t1);
    ugi.addTokenIdentifier(t2);

    Collection<TokenIdentifier> z = ugi.getTokenIdentifiers();
    assertTrue(z.contains(t1));
    assertTrue(z.contains(t2));
    assertEquals(2, z.size());

    // ensure that the token identifiers are passed through doAs
    Collection<TokenIdentifier> otherSet = ugi
        .doAs(new PrivilegedExceptionAction<Collection<TokenIdentifier>>() {
          @Override
          public Collection<TokenIdentifier> run() throws IOException {
            return UserGroupInformation.getCurrentUser().getTokenIdentifiers();
          }
        });
    assertTrue(otherSet.contains(t1));
    assertTrue(otherSet.contains(t2));
    assertEquals(2, otherSet.size());
  }

  @Test
  public void testTestAuthMethod() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    // verify the reverse mappings works
    for (AuthenticationMethod am : AuthenticationMethod.values()) {
      if (am.getAuthMethod() != null) {
        ugi.setAuthenticationMethod(am.getAuthMethod());
        assertEquals(am, ugi.getAuthenticationMethod());
      }
    }
  }
  
  @Test
  public void testUGIAuthMethod() throws Exception {
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    final AuthenticationMethod am = AuthenticationMethod.KERBEROS;
    ugi.setAuthenticationMethod(am);
    Assert.assertEquals(am, ugi.getAuthenticationMethod());
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        Assert.assertEquals(am, UserGroupInformation.getCurrentUser()
            .getAuthenticationMethod());
        return null;
      }
    });
  }
  
  @Test
  public void testUGIAuthMethodInRealUser() throws Exception {
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(
        "proxy", ugi);
    final AuthenticationMethod am = AuthenticationMethod.KERBEROS;
    ugi.setAuthenticationMethod(am);
    Assert.assertEquals(am, ugi.getAuthenticationMethod());
    Assert.assertEquals(AuthenticationMethod.PROXY,
                        proxyUgi.getAuthenticationMethod());
    Assert.assertEquals(am, UserGroupInformation
        .getRealAuthenticationMethod(proxyUgi));
    proxyUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        Assert.assertEquals(AuthenticationMethod.PROXY, UserGroupInformation
            .getCurrentUser().getAuthenticationMethod());
        Assert.assertEquals(am, UserGroupInformation.getCurrentUser()
            .getRealUser().getAuthenticationMethod());
        return null;
      }
    });
    UserGroupInformation proxyUgi2 = 
      new UserGroupInformation(proxyUgi.getSubject());
    proxyUgi2.setAuthenticationMethod(AuthenticationMethod.PROXY);
    Assert.assertEquals(proxyUgi, proxyUgi2);
    // Equality should work if authMethod is null
    UserGroupInformation realugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation proxyUgi3 = UserGroupInformation.createProxyUser(
        "proxyAnother", realugi);
    UserGroupInformation proxyUgi4 = 
      new UserGroupInformation(proxyUgi3.getSubject());
    Assert.assertEquals(proxyUgi3, proxyUgi4);
  }
  
  @Test
  public void testLoginObjectInSubject() throws Exception {
    UserGroupInformation loginUgi = UserGroupInformation.getLoginUser();
    UserGroupInformation anotherUgi = new UserGroupInformation(loginUgi
        .getSubject());
    LoginContext login1 = loginUgi.getSubject().getPrincipals(User.class)
        .iterator().next().getLogin();
    LoginContext login2 = anotherUgi.getSubject().getPrincipals(User.class)
    .iterator().next().getLogin();
    //login1 and login2 must be same instances
    Assert.assertTrue(login1 == login2);
  }
  
  @Test
  public void testLoginModuleCommit() throws Exception {
    UserGroupInformation loginUgi = UserGroupInformation.getLoginUser();
    User user1 = loginUgi.getSubject().getPrincipals(User.class).iterator()
        .next();
    LoginContext login = user1.getLogin();
    login.logout();
    login.login();
    User user2 = loginUgi.getSubject().getPrincipals(User.class).iterator()
        .next();
    // user1 and user2 must be same instances.
    Assert.assertTrue(user1 == user2);
  }
  
  public static void verifyLoginMetrics(long success, int failure)
      throws IOException {
    // Ensure metrics related to kerberos login is updated.
    MetricsRecordBuilder rb = getMetrics("UgiMetrics");
    if (success > 0) {
      assertCounter("LoginSuccessNumOps", success, rb);
      assertGaugeGt("LoginSuccessAvgTime", 0, rb);
    }
    if (failure > 0) {
      assertCounter("LoginFailureNumPos", failure, rb);
      assertGaugeGt("LoginFailureAvgTime", 0, rb);
    }
  }

  /**
   * Test for the case that UserGroupInformation.getCurrentUser()
   * is called when the AccessControlContext has a Subject associated
   * with it, but that Subject was not created by Hadoop (ie it has no
   * associated User principal)
   */
  @Test
  public void testUGIUnderNonHadoopContext() throws Exception {
    Subject nonHadoopSubject = new Subject();
    Subject.doAs(nonHadoopSubject, new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
          assertNotNull(ugi);
          return null;
        }
      });
  }
  
  @Test(timeout=1000)
  public void testSetLoginUser() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test-user");
    UserGroupInformation.setLoginUser(ugi);
    assertEquals(ugi, UserGroupInformation.getLoginUser());
  }
}
