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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * An implementation of the interface will provide authorization related
 * information and enforce permission check. It is excepted that any of the
 * methods defined in this interface should be non-blocking call and should not
 * involve expensive computation as these method could be invoked in RPC.
 */
@Private
@Unstable
public abstract class YarnAuthorizationProvider {

  private static final Log LOG = LogFactory.getLog(YarnAuthorizationProvider.class);

  private static YarnAuthorizationProvider authorizer = null;

  public static YarnAuthorizationProvider getInstance(Configuration conf) {
    synchronized (YarnAuthorizationProvider.class) {
      if (authorizer == null) {
        Class<?> authorizerClass =
            conf.getClass(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER,
              ConfiguredYarnAuthorizer.class);
        authorizer =
            (YarnAuthorizationProvider) ReflectionUtils.newInstance(
              authorizerClass, conf);
        authorizer.init(conf);
        LOG.info(authorizerClass.getName() + " is instiantiated.");
      }
    }
    return authorizer;
  }

  /**
   * Initialize the provider. Invoked on daemon startup. DefaultYarnAuthorizer is
   * initialized based on configurations.
   */
  public abstract void init(Configuration conf);

  /**
   * Check if user has the permission to access the target object.
   * 
   * @param accessType
   *          The type of accessing method.
   * @param target
   *          The target object being accessed, e.g. app/queue
   * @param user
   *          User who access the target
   * @return true if user can access the object, otherwise false.
   */
  public abstract boolean checkPermission(AccessType accessType,
      PrivilegedEntity target, UserGroupInformation user);

  /**
   * Set ACLs for the target object. AccessControlList class encapsulate the
   * users and groups who can access the target.
   *
   * @param target
   *          The target object.
   * @param acls
   *          A map from access method to a list of users and/or groups who has
   *          permission to do the access.
   * @param ugi User who sets the permissions.
   */
  public abstract void setPermission(PrivilegedEntity target,
      Map<AccessType, AccessControlList> acls, UserGroupInformation ugi);

  /**
   * Set a list of users/groups who have admin access
   * 
   * @param acls  users/groups who have admin access
   * @param ugi User who sets the admin acls.
   */
  public abstract void setAdmins(AccessControlList acls, UserGroupInformation ugi);

  /**
   * Check if the user is an admin.
   * 
   * @param ugi the user to be determined if it is an admin
   * @return true if the given user is an admin
   */
  public abstract boolean isAdmin(UserGroupInformation ugi);
}
