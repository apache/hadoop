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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;

public class TestUserGroupInformation {
  final private static String USER_NAME = "user1@HADOOP.APACHE.ORG";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String[] GROUP_NAMES = 
    new String[]{GROUP1_NAME, GROUP2_NAME, GROUP3_NAME};

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
    pp = Runtime.getRuntime().exec("id -Gn");
    br = new BufferedReader(new InputStreamReader(pp.getInputStream()));
    String line = br.readLine();
    System.out.println(userName + ":" + line);
   
    List<String> groups = new ArrayList<String> ();    
    for(String s: line.split("[\\s]")) {
      groups.add(s);
    }
    
    final UserGroupInformation login = UserGroupInformation.getCurrentUser();
    assertEquals(userName, login.getShortUserName());
    String[] gi = login.getGroupNames();
    assertEquals(groups.size(), gi.length);
    for(int i=0; i < gi.length; i++) {
      assertEquals(groups.get(i), gi[i]);
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
        public UserGroupInformation run() throws IOException {
          return UserGroupInformation.getCurrentUser();
        }});
    // make sure in the scope of the doAs, the right user is current
    assertEquals(curUGI, userGroupInfo);
    // make sure it is not the same as the login user
    assertFalse(curUGI.equals(UserGroupInformation.getLoginUser()));
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
    // The subjects should be equal, so this should work
    assertTrue(uugi.equals(
                 UserGroupInformation.createUserForTesting
                   (USER_NAME, GROUP_NAMES)));
    // ensure that different UGI with the same subject are equal
    assertEquals(uugi, new UserGroupInformation(uugi.getSubject()));
  }
  
  @Test
  public void testEqualsWithRealUser() throws Exception {
    UserGroupInformation realUgi1 = UserGroupInformation.createUserForTesting(
        "RealUser", GROUP_NAMES);
    UserGroupInformation realUgi2 = UserGroupInformation.createUserForTesting(
        "RealUser", GROUP_NAMES);
    UserGroupInformation proxyUgi1 = UserGroupInformation.createProxyUser(
        USER_NAME, realUgi1);
    UserGroupInformation proxyUgi2 = UserGroupInformation.createProxyUser(
        USER_NAME, realUgi2);
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
  public <T extends TokenIdentifier> void testUGITokens() throws Exception {
    UserGroupInformation ugi = 
      UserGroupInformation.createUserForTesting("TheDoctor", 
                                                new String [] { "TheTARDIS"});
    Token<T> t1 = mock(Token.class);
    Token<T> t2 = mock(Token.class);
    
    ugi.addToken(t1);
    ugi.addToken(t2);
    
    Collection<Token<? extends TokenIdentifier>> z = ugi.getTokens();
    assertTrue(z.contains(t1));
    assertTrue(z.contains(t2));
    assertEquals(2, z.size());
    
    try {
      z.remove(t1);
      fail("Shouldn't be able to modify token collection from UGI");
    } catch(UnsupportedOperationException uoe) {
      // Can't modify tokens
    }
    
    // ensure that the tokens are passed through doAs
    Collection<Token<? extends TokenIdentifier>> otherSet = 
      ugi.doAs(new PrivilegedExceptionAction<Collection<Token<?>>>(){
        public Collection<Token<?>> run() throws IOException {
          return UserGroupInformation.getCurrentUser().getTokens();
        }
      });
    assertTrue(otherSet.contains(t1));
    assertTrue(otherSet.contains(t2));
  }
  
  @Test
  public void testUGIAuthMethod() throws Exception {
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    final AuthenticationMethod am = AuthenticationMethod.KERBEROS;
    ugi.setAuthenticationMethod(am);
    Assert.assertEquals(am, ugi.getAuthenticationMethod());
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
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
    Assert.assertEquals(null, proxyUgi.getAuthenticationMethod());
    proxyUgi.setAuthenticationMethod(AuthenticationMethod.PROXY);
    proxyUgi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        Assert.assertEquals(AuthenticationMethod.PROXY, UserGroupInformation
            .getCurrentUser().getAuthenticationMethod());
        Assert.assertEquals(am, UserGroupInformation.getCurrentUser()
            .getRealUser().getAuthenticationMethod());
        return null;
      }
    });
    UserGroupInformation proxyUgi2 = UserGroupInformation.createProxyUser(
        "proxy", ugi);
    proxyUgi2.setAuthenticationMethod(AuthenticationMethod.PROXY);
    Assert.assertEquals(proxyUgi, proxyUgi2);
    // Equality should work if authMethod is null
    UserGroupInformation realugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation proxyUgi3 = UserGroupInformation.createProxyUser(
        "proxyAnother", realugi);
    UserGroupInformation proxyUgi4 = UserGroupInformation.createProxyUser(
        "proxyAnother", realugi);
    Assert.assertEquals(proxyUgi3, proxyUgi4);
  }
}
