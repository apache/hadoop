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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AdminACLsManager;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class ApplicationACLsManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(ApplicationACLsManager.class);

  private static AccessControlList DEFAULT_YARN_APP_ACL 
    = new AccessControlList(YarnConfiguration.DEFAULT_YARN_APP_ACL);
  private final Configuration conf;
  private final AdminACLsManager adminAclsManager;
  private final ConcurrentMap<ApplicationId, Map<ApplicationAccessType, AccessControlList>> applicationACLS
    = new ConcurrentHashMap<ApplicationId, Map<ApplicationAccessType, AccessControlList>>();

  @VisibleForTesting
  public ApplicationACLsManager() {
    this(new Configuration());
  }
  
  public ApplicationACLsManager(Configuration conf) {
    this.conf = conf;
    this.adminAclsManager = new AdminACLsManager(this.conf);
  }

  public boolean areACLsEnabled() {
    return adminAclsManager.areACLsEnabled();
  }

  public void addApplication(ApplicationId appId,
      Map<ApplicationAccessType, String> acls) {
    Map<ApplicationAccessType, AccessControlList> finalMap
        = new HashMap<ApplicationAccessType, AccessControlList>(acls.size());
    for (Entry<ApplicationAccessType, String> acl : acls.entrySet()) {
      finalMap.put(acl.getKey(), new AccessControlList(acl.getValue()));
    }
    this.applicationACLS.put(appId, finalMap);
  }

  public void removeApplication(ApplicationId appId) {
    this.applicationACLS.remove(appId);
  }

  /**
   * If authorization is enabled, checks whether the user (in the callerUGI) is
   * authorized to perform the access specified by 'applicationAccessType' on
   * the application by checking if the user is applicationOwner or part of
   * application ACL for the specific access-type.
   * <ul>
   * <li>The owner of the application can have all access-types on the
   * application</li>
   * <li>For all other users/groups application-acls are checked</li>
   * </ul>
   * 
   * @param callerUGI
   * @param applicationAccessType
   * @param applicationOwner
   * @param applicationId
   */
  public boolean checkAccess(UserGroupInformation callerUGI,
      ApplicationAccessType applicationAccessType, String applicationOwner,
      ApplicationId applicationId) {

    LOG.debug("Verifying access-type {} for {} on application {} owned by {}",
            applicationAccessType, callerUGI, applicationId, applicationOwner);

    String user = callerUGI.getShortUserName();
    if (!areACLsEnabled()) {
      return true;
    }
    AccessControlList applicationACL = DEFAULT_YARN_APP_ACL;
    Map<ApplicationAccessType, AccessControlList> acls = this.applicationACLS
        .get(applicationId);
    if (acls == null) {
      LOG.debug("ACL not found for application {} owned by {}."
          + " Using default [{}]", applicationId, applicationOwner,
          YarnConfiguration.DEFAULT_YARN_APP_ACL);
    } else {
      AccessControlList applicationACLInMap = acls.get(applicationAccessType);
      if (applicationACLInMap != null) {
        applicationACL = applicationACLInMap;
      } else {
        LOG.debug("ACL not found for access-type {} for application {}"
            + " owned by {}. Using default [{}]", applicationAccessType,
            applicationId, applicationOwner,
            YarnConfiguration.DEFAULT_YARN_APP_ACL);
      }
    }

    // Allow application-owner for any type of access on the application
    if (this.adminAclsManager.isAdmin(callerUGI)
        || user.equals(applicationOwner)
        || applicationACL.isUserAllowed(callerUGI)) {
      return true;
    }
    return false;
  }

  /**
   * Check if the given user in an admin.
   *
   * @param calledUGI
   *          UserGroupInformation for the user
   * @return true if the user is an admin, false otherwise
   */
  public final boolean isAdmin(final UserGroupInformation calledUGI) {
    return this.adminAclsManager.isAdmin(calledUGI);
  }
}
