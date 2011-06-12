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
import org.apache.hadoop.io.TestWritable;

import junit.framework.TestCase;

/** Unit tests for UnixUserGroupInformation */
public class TestUnixUserGroupInformation extends TestCase {
  final private static String USER_NAME = "user1";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String[] GROUP_NAMES = 
                      new String[]{GROUP1_NAME, GROUP2_NAME, GROUP3_NAME};
  
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
    testConstructorFailures(USER_NAME, new String[0]);
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
    TestWritable.testWritable(ugi, new Configuration());
  }
}
