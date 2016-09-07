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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.junit.*;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;
import static org.apache.hadoop.ipc.TestSaslRPC.*;
import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUserGroupInformation {
  final private static String USER_NAME = "user1@HADOOP.APACHE.ORG";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String[] GROUP_NAMES = 
    new String[]{GROUP1_NAME, GROUP2_NAME, GROUP3_NAME};
  // Rollover interval of percentile metrics (in seconds)
  private static final int PERCENTILES_INTERVAL = 1;
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
    // doesn't matter what it is, but getGroups needs it set...
    // use HADOOP_HOME environment variable to prevent interfering with logic
    // that finds winutils.exe
    String home = System.getenv("HADOOP_HOME");
    System.setProperty("hadoop.home.dir", (home != null ? home : "."));
    // fake the realm is kerberos is enabled
    System.setProperty("java.security.krb5.kdc", "");
    System.setProperty("java.security.krb5.realm", "DEFAULT.REALM");
  }
  
  @Before
  public void setupUgi() {
    conf = new Configuration();
    UserGroupInformation.reset();
    UserGroupInformation.setConfiguration(conf);
  }
  
  @After
  public void resetUgi() {
    UserGroupInformation.setLoginUser(null);
  }

  @Test (timeout = 30000)
  public void testSimpleLogin() throws IOException {
    tryLoginAuthenticationMethod(AuthenticationMethod.SIMPLE, true);
  }

  @Test (timeout = 30000)
  public void testTokenLogin() throws IOException {
    tryLoginAuthenticationMethod(AuthenticationMethod.TOKEN, false);
  }
  
  @Test (timeout = 30000)
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
  
  @Test (timeout = 30000)
  public void testGetRealAuthenticationMethod() {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("user1");
    ugi.setAuthenticationMethod(AuthenticationMethod.SIMPLE);
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getRealAuthenticationMethod());
    ugi = UserGroupInformation.createProxyUser("user2", ugi);
    assertEquals(AuthenticationMethod.PROXY, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getRealAuthenticationMethod());
  }
  
  @Test (timeout = 30000)
  public void testCreateRemoteUser() {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("user1");
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getAuthenticationMethod());
    assertTrue (ugi.toString().contains("(auth:SIMPLE)"));
    ugi = UserGroupInformation.createRemoteUser("user1", 
        AuthMethod.KERBEROS);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertTrue (ugi.toString().contains("(auth:KERBEROS)"));
  }
  
  /** Test login method */
  @Test (timeout = 30000)
  public void testLogin() throws Exception {
    conf.set(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS,
      String.valueOf(PERCENTILES_INTERVAL));
    UserGroupInformation.setConfiguration(conf);
    // login from unix
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    assertEquals(UserGroupInformation.getCurrentUser(),
                 UserGroupInformation.getLoginUser());
    assertTrue(ugi.getGroupNames().length >= 1);
    verifyGroupMetrics(1);

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
  @Test (timeout = 30000)
  public void testGetServerSideGroups() throws IOException,
                                               InterruptedException {
    // get the user name
    Process pp = Runtime.getRuntime().exec("whoami");
    BufferedReader br = new BufferedReader
                          (new InputStreamReader(pp.getInputStream()));
    String userName = br.readLine().trim();
    // If on windows domain, token format is DOMAIN\\user and we want to
    // extract only the user name
    if(Shell.WINDOWS) {
      int sp = userName.lastIndexOf('\\');
      if (sp != -1) {
        userName = userName.substring(sp + 1);
      }
      // user names are case insensitive on Windows. Make consistent
      userName = StringUtils.toLowerCase(userName);
    }
    // get the groups
    pp = Runtime.getRuntime().exec(Shell.WINDOWS ?
      Shell.WINUTILS + " groups -F"
      : "id -Gn " + userName);
    br = new BufferedReader(new InputStreamReader(pp.getInputStream()));
    String line = br.readLine();

    System.out.println(userName + ":" + line);
   
    Set<String> groups = new LinkedHashSet<String> ();    
    String[] tokens = line.split(Shell.TOKEN_SEPARATOR_REGEX);
    for(String s: tokens) {
      groups.add(s);
    }
    
    final UserGroupInformation login = UserGroupInformation.getCurrentUser();
    String loginUserName = login.getShortUserName();
    if(Shell.WINDOWS) {
      // user names are case insensitive on Windows. Make consistent
      loginUserName = StringUtils.toLowerCase(loginUserName);
    }
    assertEquals(userName, loginUserName);

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
  @Test (timeout = 30000)
  public void testConstructor() throws Exception {
    // security off, so default should just return simple name
    testConstructorSuccess("user1", "user1");
    testConstructorSuccess("user2@DEFAULT.REALM", "user2");
    testConstructorSuccess("user3/cron@DEFAULT.REALM", "user3");    
    testConstructorSuccess("user4@OTHER.REALM", "user4");
    testConstructorSuccess("user5/cron@OTHER.REALM", "user5");
    // failure test
    testConstructorFailures(null);
    testConstructorFailures("");
  }
  
  /** test constructor */
  @Test (timeout = 30000)
  public void testConstructorWithRules() throws Exception {
    // security off, but use rules if explicitly set
    conf.set(HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[1:$1@$0](.*@OTHER.REALM)s/(.*)@.*/other-$1/");
    UserGroupInformation.setConfiguration(conf);
    testConstructorSuccess("user1", "user1");
    testConstructorSuccess("user4@OTHER.REALM", "other-user4");
    // failure test
    testConstructorFailures("user2@DEFAULT.REALM");
    testConstructorFailures("user3/cron@DEFAULT.REALM");
    testConstructorFailures("user5/cron@OTHER.REALM");
    testConstructorFailures(null);
    testConstructorFailures("");
  }
  
  /** test constructor */
  @Test (timeout = 30000)
  public void testConstructorWithKerberos() throws Exception {
    // security on, default is remove default realm
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    testConstructorSuccess("user1", "user1");
    testConstructorSuccess("user2@DEFAULT.REALM", "user2");
    testConstructorSuccess("user3/cron@DEFAULT.REALM", "user3");    
    // failure test
    testConstructorFailures("user4@OTHER.REALM");
    testConstructorFailures("user5/cron@OTHER.REALM");
    testConstructorFailures(null);
    testConstructorFailures("");
  }

  /** test constructor */
  @Test (timeout = 30000)
  public void testConstructorWithKerberosRules() throws Exception {
    // security on, explicit rules
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    conf.set(HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[2:$1@$0](.*@OTHER.REALM)s/(.*)@.*/other-$1/" +
        "RULE:[1:$1@$0](.*@OTHER.REALM)s/(.*)@.*/other-$1/" +
        "DEFAULT");
    UserGroupInformation.setConfiguration(conf);
    
    testConstructorSuccess("user1", "user1");
    testConstructorSuccess("user2@DEFAULT.REALM", "user2");
    testConstructorSuccess("user3/cron@DEFAULT.REALM", "user3");    
    testConstructorSuccess("user4@OTHER.REALM", "other-user4");
    testConstructorSuccess("user5/cron@OTHER.REALM", "other-user5");
    // failure test
    testConstructorFailures(null);
    testConstructorFailures("");
  }

  private void testConstructorSuccess(String principal, String shortName) {
    UserGroupInformation ugi = 
        UserGroupInformation.createUserForTesting(principal, GROUP_NAMES);
    // make sure the short and full user names are correct
    assertEquals(principal, ugi.getUserName());
    assertEquals(shortName, ugi.getShortUserName());
  }
  
  private void testConstructorFailures(String userName) {
    try {
      UserGroupInformation.createRemoteUser(userName);
      fail("user:"+userName+" wasn't invalid");
    } catch (IllegalArgumentException e) {
      String expect = (userName == null || userName.isEmpty())
          ? "Null user" : "Illegal principal name "+userName;
      assertTrue("Did not find "+ expect + " in " + e,
          e.toString().contains(expect));
    }
  }

  @Test (timeout = 30000)
  public void testSetConfigWithRules() {
    String[] rules = { "RULE:[1:TEST1]", "RULE:[1:TEST2]", "RULE:[1:TEST3]" };

    // explicitly set a rule
    UserGroupInformation.reset();
    assertFalse(KerberosName.hasRulesBeenSet());
    KerberosName.setRules(rules[0]);
    assertTrue(KerberosName.hasRulesBeenSet());
    assertEquals(rules[0], KerberosName.getRules());

    // implicit init should honor rules already being set
    UserGroupInformation.createUserForTesting("someone", new String[0]);
    assertEquals(rules[0], KerberosName.getRules());

    // set conf, should override
    conf.set(HADOOP_SECURITY_AUTH_TO_LOCAL, rules[1]);
    UserGroupInformation.setConfiguration(conf);
    assertEquals(rules[1], KerberosName.getRules());

    // set conf, should again override
    conf.set(HADOOP_SECURITY_AUTH_TO_LOCAL, rules[2]);
    UserGroupInformation.setConfiguration(conf);
    assertEquals(rules[2], KerberosName.getRules());
    
    // implicit init should honor rules already being set
    UserGroupInformation.createUserForTesting("someone", new String[0]);
    assertEquals(rules[2], KerberosName.getRules());
  }

  @Test (timeout = 30000)
  public void testEnsureInitWithRules() throws IOException {
    String rules = "RULE:[1:RULE1]";

    // trigger implicit init, rules should init
    UserGroupInformation.reset();
    assertFalse(KerberosName.hasRulesBeenSet());
    UserGroupInformation.createUserForTesting("someone", new String[0]);
    assertTrue(KerberosName.hasRulesBeenSet());
    
    // set a rule, trigger implicit init, rule should not change 
    UserGroupInformation.reset();
    KerberosName.setRules(rules);
    assertTrue(KerberosName.hasRulesBeenSet());
    assertEquals(rules, KerberosName.getRules());
    UserGroupInformation.createUserForTesting("someone", new String[0]);
    assertEquals(rules, KerberosName.getRules());
  }

  @Test (timeout = 30000)
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
  
  @Test (timeout = 30000)
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
  
  @Test (timeout = 30000)
  public void testGettingGroups() throws Exception {
    UserGroupInformation uugi = 
      UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES);
    assertEquals(USER_NAME, uugi.getUserName());
    assertArrayEquals(new String[]{GROUP1_NAME, GROUP2_NAME, GROUP3_NAME},
                      uugi.getGroupNames());
  }

  @SuppressWarnings("unchecked") // from Mockito mocks
  @Test (timeout = 30000)
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
  @Test (timeout = 30000)
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
  @Test (timeout = 30000)
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

  @Test (timeout = 30000)
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
  @Test (timeout = 30000)
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
  @Test (timeout = 30000)
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
  
  @Test (timeout = 30000)
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

  @Test (timeout = 30000)
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
  
  @Test (timeout = 30000)
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
  
  @Test (timeout = 30000)
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
  
  @Test (timeout = 30000)
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
  
  @Test (timeout = 30000)
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

  private static void verifyGroupMetrics(
      long groups) throws InterruptedException {
    MetricsRecordBuilder rb = getMetrics("UgiMetrics");
    if (groups > 0) {
      assertCounterGt("GetGroupsNumOps", groups-1, rb);
      double avg = getDoubleGauge("GetGroupsAvgTime", rb);
      assertTrue(avg >= 0.0);

      // Sleep for an interval+slop to let the percentiles rollover
      Thread.sleep((PERCENTILES_INTERVAL+1)*1000);
      // Check that the percentiles were updated
      assertQuantileGauges("GetGroups1s", rb);
    }
  }

  /**
   * Test for the case that UserGroupInformation.getCurrentUser()
   * is called when the AccessControlContext has a Subject associated
   * with it, but that Subject was not created by Hadoop (ie it has no
   * associated User principal)
   */
  @Test (timeout = 30000)
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

  @Test (timeout = 30000)
  public void testGetUGIFromSubject() throws Exception {
    KerberosPrincipal p = new KerberosPrincipal("guest");
    Subject subject = new Subject();
    subject.getPrincipals().add(p);
    UserGroupInformation ugi = UserGroupInformation.getUGIFromSubject(subject);
    assertNotNull(ugi);
    assertEquals("guest@DEFAULT.REALM", ugi.getUserName());
  }

  @Test(timeout=1000)
  public void testSetLoginUser() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test-user");
    UserGroupInformation.setLoginUser(ugi);
    assertEquals(ugi, UserGroupInformation.getLoginUser());
  }

  /**
   * In some scenario, such as HA, delegation tokens are associated with a
   * logical name. The tokens are cloned and are associated with the
   * physical address of the server where the service is provided.
   * This test ensures cloned delegated tokens are locally used
   * and are not returned in {@link UserGroupInformation#getCredentials()}
   */
  @Test
  public void testPrivateTokenExclusion() throws Exception  {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    TestTokenIdentifier tokenId = new TestTokenIdentifier();
    Token<TestTokenIdentifier> token = new Token<TestTokenIdentifier>(
            tokenId.getBytes(), "password".getBytes(),
            tokenId.getKind(), null);
    ugi.addToken(new Text("regular-token"), token);

    // Now add cloned private token
    ugi.addToken(new Text("private-token"), new Token.PrivateToken<TestTokenIdentifier>(token));
    ugi.addToken(new Text("private-token1"), new Token.PrivateToken<TestTokenIdentifier>(token));

    // Ensure only non-private tokens are returned
    Collection<Token<? extends TokenIdentifier>> tokens = ugi.getCredentials().getAllTokens();
    assertEquals(1, tokens.size());
  }

  /**
   * This test checks a race condition between getting and adding tokens for
   * the current user.  Calling UserGroupInformation.getCurrentUser() returns
   * a new object each time, so simply making these methods synchronized was not
   * enough to prevent race conditions and causing a
   * ConcurrentModificationException.  These methods are synchronized on the
   * Subject, which is the same object between UserGroupInformation instances.
   * This test tries to cause a CME, by exposing the race condition.  Previously
   * this test would fail every time; now it does not.
   */
  @Test
  public void testTokenRaceCondition() throws Exception {
    UserGroupInformation userGroupInfo =
      UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES);
    userGroupInfo.doAs(new PrivilegedExceptionAction<Void>(){
      @Override
      public Void run() throws Exception {
        // make sure it is not the same as the login user because we use the
        // same UGI object for every instantiation of the login user and you
        // won't run into the race condition otherwise
        assertNotEquals(UserGroupInformation.getLoginUser(),
                        UserGroupInformation.getCurrentUser());

        GetTokenThread thread = new GetTokenThread();
        try {
          thread.start();
          for (int i = 0; i < 100; i++) {
            @SuppressWarnings("unchecked")
            Token<? extends TokenIdentifier> t = mock(Token.class);
            when(t.getService()).thenReturn(new Text("t" + i));
            UserGroupInformation.getCurrentUser().addToken(t);
            assertNull("ConcurrentModificationException encountered",
                thread.cme);
          }
        } catch (ConcurrentModificationException cme) {
          cme.printStackTrace();
          fail("ConcurrentModificationException encountered");
        } finally {
          thread.runThread = false;
          thread.join(5 * 1000);
        }
        return null;
      }});
  }

  static class GetTokenThread extends Thread {
    boolean runThread = true;
    volatile ConcurrentModificationException cme = null;

    @Override
    public void run() {
      while(runThread) {
        try {
          UserGroupInformation.getCurrentUser().getCredentials();
        } catch (ConcurrentModificationException cme) {
          this.cme = cme;
          cme.printStackTrace();
          runThread = false;
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testCheckTGTAfterLoginFromSubject() throws Exception {
    // security on, default is remove default realm
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    // Login from a pre-set subject with a keytab
    final Subject subject = new Subject();
    KeyTab keytab = KeyTab.getInstance();
    subject.getPrivateCredentials().add(keytab);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws IOException {
        UserGroupInformation.loginUserFromSubject(subject);
        // this should not throw.
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        return null;
      }
    });

  }
}
