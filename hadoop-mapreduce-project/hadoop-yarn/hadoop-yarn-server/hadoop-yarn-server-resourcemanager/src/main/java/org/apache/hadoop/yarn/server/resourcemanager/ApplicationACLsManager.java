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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@InterfaceAudience.Private
public class ApplicationACLsManager {

  Configuration conf;

  public ApplicationACLsManager(Configuration conf) {
    this.conf = conf;
  }

  public boolean areACLsEnabled() {
    return conf.getBoolean(YarnConfiguration.RM_ACL_ENABLE,
        YarnConfiguration.DEFAULT_RM_ACL_ENABLE);
  }

  /**
   * Construct the ApplicationACLs from the configuration so that they can be kept in
   * the memory. If authorization is disabled on the RM, nothing is constructed
   * and an empty map is returned.
   * 
   * @return ApplicationACL to AccessControlList map.
   */
  public Map<ApplicationACL, AccessControlList> constructApplicationACLs(
      Configuration conf) {

    Map<ApplicationACL, AccessControlList> acls =
        new HashMap<ApplicationACL, AccessControlList>();

    // Don't construct anything if authorization is disabled.
    if (!areACLsEnabled()) {
      return acls;
    }

    for (ApplicationACL aclName : ApplicationACL.values()) {
      String aclConfigName = aclName.getAclName();
      String aclConfigured = conf.get(aclConfigName);
      if (aclConfigured == null) {
        // If ACLs are not configured at all, we grant no access to anyone. So
        // applicationOwner and superuser/supergroup _only_ can do 'stuff'
        aclConfigured = " ";
      }
      acls.put(aclName, new AccessControlList(aclConfigured));
    }
    return acls;
  }

  /**
   * If authorization is enabled, checks whether the user (in the callerUGI)
   * is authorized to perform the operation specified by 'applicationOperation' on
   * the application by checking if the user is applicationOwner or part of application ACL for the
   * specific application operation.
   * <ul>
   * <li>The owner of the application can do any operation on the application</li>
   * <li>For all other users/groups application-acls are checked</li>
   * </ul>
   * @param callerUGI
   * @param applicationOperation
   * @param applicationOwner
   * @param acl
   * @throws AccessControlException
   */
  public boolean checkAccess(UserGroupInformation callerUGI,
      ApplicationACL applicationOperation, String applicationOwner,
      AccessControlList acl) {

    String user = callerUGI.getShortUserName();
    if (!areACLsEnabled()) {
      return true;
    }

    // Allow application-owner for any operation on the application
    if (user.equals(applicationOwner)
        || acl.isUserAllowed(callerUGI)) {
      return true;
    }

    return false;
  }
}
