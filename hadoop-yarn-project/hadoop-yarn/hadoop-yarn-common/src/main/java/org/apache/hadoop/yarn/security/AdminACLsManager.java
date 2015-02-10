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

package org.apache.hadoop.yarn.security;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

@Private
public class AdminACLsManager {

  /**
   * Log object for this class
   */
  static Log LOG = LogFactory.getLog(AdminACLsManager.class);

  /**
   * The current user at the time of object creation
   */
  private final UserGroupInformation owner;

  /**
   * Holds list of administrator users
   */
  private final AccessControlList adminAcl;

  /**
   * True if ACLs are enabled
   *
   * @see YarnConfiguration#YARN_ACL_ENABLE
   * @see YarnConfiguration#DEFAULT_YARN_ACL_ENABLE
   */
  private final boolean aclsEnabled;

  /**
   * Constructs and initializes this AdminACLsManager
   *
   * @param conf configuration for this object to use
   */
  public AdminACLsManager(Configuration conf) {

    this.adminAcl = new AccessControlList(conf.get(
          YarnConfiguration.YARN_ADMIN_ACL,
          YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    try {
      owner = UserGroupInformation.getCurrentUser();
      adminAcl.addUser(owner.getShortUserName());
    } catch (IOException e){
      LOG.warn("Could not add current user to admin:" + e);
      throw new YarnRuntimeException(e);
    }

    aclsEnabled = conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
  }

  /**
   * Returns the owner
   *
   * @return Current user at the time of object creation
   */
  public UserGroupInformation getOwner() {
    return owner;
  }

  /**
   * Returns whether ACLs are enabled
   *
   * @see YarnConfiguration#YARN_ACL_ENABLE
   * @see YarnConfiguration#DEFAULT_YARN_ACL_ENABLE
   * @return <tt>true</tt> if ACLs are enabled
   */
  public boolean areACLsEnabled() {
    return aclsEnabled;
  }

  /**
   * Returns whether the specified user/group is an administrator
   *
   * @param callerUGI user/group to to check
   * @return <tt>true</tt> if the UserGroupInformation specified
   *         is a member of the access control list for administrators
   */
  public boolean isAdmin(UserGroupInformation callerUGI) {
    return adminAcl.isUserAllowed(callerUGI);
  }
}
