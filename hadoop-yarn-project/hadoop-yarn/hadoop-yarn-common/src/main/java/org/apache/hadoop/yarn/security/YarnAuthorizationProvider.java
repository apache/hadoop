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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;

/**
 * An implementation of the interface will provide authorization related
 * information and enforce permission check. It is excepted that any of the
 * methods defined in this interface should be non-blocking call and should not
 * involve expensive computation as these method could be invoked in RPC.
 */
@Private
@Unstable
public abstract class YarnAuthorizationProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(YarnAuthorizationProvider.class);

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
        LOG.info(authorizerClass.getName() + " is instantiated.");
      }
    }
    return authorizer;
  }

  /**
   * Destroy the {@link YarnAuthorizationProvider} instance.
   * This method is called only in Tests.
   */
  @VisibleForTesting
  public static void destroy() {
    synchronized (YarnAuthorizationProvider.class) {
      if (authorizer != null) {
        LOG.debug("{} is destroyed.", authorizer.getClass().getName());
        authorizer = null;
      }
    }
  }

  /**
   * Initialize the provider. Invoked on daemon startup. DefaultYarnAuthorizer is
   * initialized based on configurations.
   */
  public abstract void init(Configuration conf);

  /**
   * Check if user has the permission to access the target object.
   * 
   * @param accessRequest
   *          the request object which contains all the access context info.
   * @return true if user can access the object, otherwise false.
   */

  public abstract boolean checkPermission(AccessRequest accessRequest);

  /**
   * Set permissions for the target object.
   *
   * @param permissions
   *        A list of permissions on the target object.
   * @param ugi User who sets the permissions.
   */
  public abstract void setPermission(List<Permission> permissions,
      UserGroupInformation ugi);

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
