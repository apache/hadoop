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
package org.apache.hadoop.yarn.server.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

public class TestApplicationACLsManager {

  private static final String ADMIN_USER = "adminuser";
  private static final String APP_OWNER = "appuser";
  private static final String TESTUSER1 = "testuser1";
  private static final String TESTUSER2 = "testuser2";
  private static final String TESTUSER3 = "testuser3";

  @Test
  public void testCheckAccess() {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL,
        ADMIN_USER);
    ApplicationACLsManager aclManager = new ApplicationACLsManager(conf);
    Map<ApplicationAccessType, String> aclMap = 
        new HashMap<ApplicationAccessType, String>();
    aclMap.put(ApplicationAccessType.VIEW_APP, TESTUSER1 + "," + TESTUSER3);
    aclMap.put(ApplicationAccessType.MODIFY_APP, TESTUSER1);
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    aclManager.addApplication(appId, aclMap);

    //User in ACL, should be allowed access
    UserGroupInformation testUser1 = UserGroupInformation
        .createRemoteUser(TESTUSER1);
    assertTrue(aclManager.checkAccess(testUser1, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertTrue(aclManager.checkAccess(testUser1, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));

    //User NOT in ACL, should not be allowed access
    UserGroupInformation testUser2 = UserGroupInformation
        .createRemoteUser(TESTUSER2);
    assertFalse(aclManager.checkAccess(testUser2, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertFalse(aclManager.checkAccess(testUser2, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));

    //User has View access, but not modify access
    UserGroupInformation testUser3 = UserGroupInformation
        .createRemoteUser(TESTUSER3);
    assertTrue(aclManager.checkAccess(testUser3, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertFalse(aclManager.checkAccess(testUser3, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));

    //Application Owner should have all access
    UserGroupInformation appOwner = UserGroupInformation
        .createRemoteUser(APP_OWNER);
    assertTrue(aclManager.checkAccess(appOwner, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertTrue(aclManager.checkAccess(appOwner, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));

    //Admin should have all access
    UserGroupInformation adminUser = UserGroupInformation
        .createRemoteUser(ADMIN_USER);
    assertTrue(aclManager.checkAccess(adminUser, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertTrue(aclManager.checkAccess(adminUser, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));
  }

  @Test
  public void testCheckAccessWithNullACLS() {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL,
        ADMIN_USER);
    ApplicationACLsManager aclManager = new ApplicationACLsManager(conf);
    UserGroupInformation appOwner = UserGroupInformation
        .createRemoteUser(APP_OWNER);
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    //Application ACL is not added

    //Application Owner should have all access even if Application ACL is not added
    assertTrue(aclManager.checkAccess(appOwner, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));
    assertTrue(aclManager.checkAccess(appOwner, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));

    //Admin should have all access
    UserGroupInformation adminUser = UserGroupInformation
        .createRemoteUser(ADMIN_USER);
    assertTrue(aclManager.checkAccess(adminUser, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertTrue(aclManager.checkAccess(adminUser, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));

    // A regular user should Not have access
    UserGroupInformation testUser1 = UserGroupInformation
        .createRemoteUser(TESTUSER1);
    assertFalse(aclManager.checkAccess(testUser1, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertFalse(aclManager.checkAccess(testUser1, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));
  }
  
  @Test
  public void testCheckAccessWithPartialACLS() {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL,
        ADMIN_USER);
    ApplicationACLsManager aclManager = new ApplicationACLsManager(conf);
    UserGroupInformation appOwner = UserGroupInformation
        .createRemoteUser(APP_OWNER);
    // Add only the VIEW ACLS
    Map<ApplicationAccessType, String> aclMap = 
        new HashMap<ApplicationAccessType, String>();
    aclMap.put(ApplicationAccessType.VIEW_APP, TESTUSER1 );
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    aclManager.addApplication(appId, aclMap);

    //Application Owner should have all access even if Application ACL is not added
    assertTrue(aclManager.checkAccess(appOwner, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));
    assertTrue(aclManager.checkAccess(appOwner, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));

    //Admin should have all access
    UserGroupInformation adminUser = UserGroupInformation
        .createRemoteUser(ADMIN_USER);
    assertTrue(aclManager.checkAccess(adminUser, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertTrue(aclManager.checkAccess(adminUser, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));

    // testuser1 should  have view access only
    UserGroupInformation testUser1 = UserGroupInformation
        .createRemoteUser(TESTUSER1);
    assertTrue(aclManager.checkAccess(testUser1, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertFalse(aclManager.checkAccess(testUser1, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));
    
    // A testuser2 should Not have access
    UserGroupInformation testUser2 = UserGroupInformation
        .createRemoteUser(TESTUSER2);
    assertFalse(aclManager.checkAccess(testUser2, ApplicationAccessType.VIEW_APP, 
        APP_OWNER, appId));
    assertFalse(aclManager.checkAccess(testUser2, ApplicationAccessType.MODIFY_APP, 
        APP_OWNER, appId));
  }
}
