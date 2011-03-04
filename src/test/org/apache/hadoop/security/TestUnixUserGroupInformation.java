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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.TestWritable;
import org.apache.hadoop.util.Shell;

/** Unit tests for UnixUserGroupInformation */
public class TestUnixUserGroupInformation extends TestCase {
  final private static String USER_NAME = "user1";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String[] GROUP_NAMES = 
                      new String[]{GROUP1_NAME, GROUP2_NAME, GROUP3_NAME};
  final private static Configuration conf = new Configuration();
  
  /** Test login method */
  public void testLogin() throws Exception {
    Configuration conf = new Configuration();
    
    // loin from unix
    String userName = UnixUserGroupInformation.getUnixUserName();
    UnixUserGroupInformation curUserGroupInfo = 
        UnixUserGroupInformation.login(conf);
    assertEquals(curUserGroupInfo.getUserName(), userName);
    assertTrue(curUserGroupInfo == UnixUserGroupInformation.login(conf));
    
    // login from the configuration
    UnixUserGroupInformation userGroupInfo = new UnixUserGroupInformation(
        USER_NAME, GROUP_NAMES );
    UnixUserGroupInformation.saveToConf(conf, 
        UnixUserGroupInformation.UGI_PROPERTY_NAME, userGroupInfo);
    curUserGroupInfo = UnixUserGroupInformation.login(conf);
    assertEquals(curUserGroupInfo, userGroupInfo);
    assertTrue(curUserGroupInfo == UnixUserGroupInformation.login(conf));
  }

  /** test constructor */
  public void testConstructor() throws Exception {
    UnixUserGroupInformation uugi = 
      new UnixUserGroupInformation(USER_NAME, GROUP_NAMES);
    assertEquals(uugi, new UnixUserGroupInformation( new String[]{
       USER_NAME, GROUP1_NAME, GROUP2_NAME, GROUP3_NAME} ));  
    // failure test
    testConstructorFailures(null, GROUP_NAMES);
    testConstructorFailures("", GROUP_NAMES);
    testConstructorFailures(USER_NAME, null);
    testConstructorFailures(USER_NAME, new String[]{null});
    testConstructorFailures(USER_NAME, new String[]{""});
    testConstructorFailures(USER_NAME, new String[]{GROUP1_NAME, null});
    testConstructorFailures(USER_NAME, 
        new String[]{GROUP1_NAME, null, GROUP2_NAME});
  }
  
  private void testConstructorFailures(String userName, String[] groupNames) {
    boolean gotException = false;
    try {
      new UnixUserGroupInformation(userName, groupNames);
    } catch (Exception e) {
      gotException = true;
    }
    assertTrue(gotException);
  }
  
  public void testEquals() throws Exception {
    UnixUserGroupInformation uugi = 
      new UnixUserGroupInformation(USER_NAME, GROUP_NAMES);

    assertEquals(uugi, uugi);
    assertEquals(uugi, new UnixUserGroupInformation(USER_NAME, GROUP_NAMES));
    assertEquals(uugi, new UnixUserGroupInformation(USER_NAME,
        new String[]{GROUP1_NAME, GROUP3_NAME, GROUP2_NAME}));
    assertFalse(uugi.equals(new UnixUserGroupInformation()));
    assertFalse(uugi.equals(new UnixUserGroupInformation(USER_NAME,
        new String[]{GROUP2_NAME, GROUP3_NAME, GROUP1_NAME})));
  }
  
  /** test Writable */
  public void testWritable() throws Exception {
    UnixUserGroupInformation ugi = new UnixUserGroupInformation(
        USER_NAME, GROUP_NAMES);
    TestWritable.testWritable(ugi, conf);
  }
  
  /**
   * given user name - get all the groups.
   */
  public void testGetServerSideGroups() throws IOException {
    
    // get the user name
    Process pp = Runtime.getRuntime().exec("whoami");
    BufferedReader br = new BufferedReader(new InputStreamReader(pp.getInputStream()));
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
    
    boolean ugiIsIn = false;
    
    // get groups on the server side
    int numberOfGroups = 0;
    Subject subject = SecurityUtil.getSubject(conf, userName);
    System.out.println("for user="+userName+" prinicipals are:");
    for(Principal p : subject.getPrincipals()) {
      if(p instanceof User) {
        System.out.println("USER: " + p.getName());
        assertTrue("user name is not the same as in the Subject: " + p.getName(),
            userName.equals(p.getName()));
      }
      if(p instanceof Group) {
        numberOfGroups++;
        System.out.println("GROUP: " + p.getName());
        assertTrue("Subject contains invalid group " + p.getName(), 
            groups.contains(p.getName()));
      }
      if(p instanceof UserGroupInformation) {
        System.out.println("UGI: " + p.getName());
        ugiIsIn = true;
      }
    }
    assertTrue("UGI object is not in the Subject", ugiIsIn);
    assertEquals("number of groups in subject doesn't match actual # groups", 
        numberOfGroups, groups.size());
    
    // negative test - get Subject for non-existing user
    // should return empty groups
    subject = SecurityUtil.getSubject(conf, "fakeUser");
    for(Principal p : subject.getPrincipals()) {
      if(p instanceof User) {
        System.out.println("USER: " + p.getName());
        assertTrue("user name (fakeUser) is not the same as in the Subject: " +
            p.getName(), "fakeUser".equals(p.getName()));
      }
      if(p instanceof Group) {
        fail("fakeUser should have no groups");
      }
    }
    
  }
}
