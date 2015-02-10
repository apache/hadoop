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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;

/**
 * A YarnAuthorizationProvider implementation based on configuration files.
 *
 */
@Private
@Unstable
public class ConfiguredYarnAuthorizer extends YarnAuthorizationProvider {

  private final ConcurrentMap<PrivilegedEntity, Map<AccessType, AccessControlList>> allAcls =
      new ConcurrentHashMap<>();
  private volatile AccessControlList adminAcl = null;


  @Override
  public void init(Configuration conf) {
    adminAcl =
        new AccessControlList(conf.get(YarnConfiguration.YARN_ADMIN_ACL,
          YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
  }

  @Override
  public void setPermission(PrivilegedEntity target,
      Map<AccessType, AccessControlList> acls, UserGroupInformation ugi) {
    allAcls.put(target, acls);
  }

  @Override
  public boolean checkPermission(AccessType accessType,
      PrivilegedEntity target, UserGroupInformation user) {
    boolean ret = false;
    Map<AccessType, AccessControlList> acls = allAcls.get(target);
    if (acls != null) {
      AccessControlList list = acls.get(accessType);
      if (list != null) {
        ret = list.isUserAllowed(user);
      }
    }

    // recursively look up the queue to see if parent queue has the permission.
    if (target.getType() == EntityType.QUEUE && !ret) {
      String queueName = target.getName();
      if (!queueName.contains(".")) {
        return ret;
      }
      String parentQueueName = queueName.substring(0, queueName.lastIndexOf("."));
      return checkPermission(accessType, new PrivilegedEntity(target.getType(),
        parentQueueName), user);
    }
    return ret;
  }

  @Override
  public void setAdmins(AccessControlList acls, UserGroupInformation ugi) {
    adminAcl = acls;
  }

  @Override
  public boolean isAdmin(UserGroupInformation ugi) {
    return adminAcl.isUserAllowed(ugi);
  }

  public AccessControlList getAdminAcls() {
    return this.adminAcl;
  }
}
