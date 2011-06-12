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
package org.apache.hadoop.security.authorize;

import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Policy;
import java.security.Principal;
import java.security.ProtectionDomain;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Group;
import org.apache.hadoop.security.User;
import org.apache.hadoop.security.SecurityUtil.AccessControlList;

/**
 * A {@link Configuration} based security {@link Policy} for Hadoop.
 *
 * {@link ConfiguredPolicy} works in conjunction with a {@link PolicyProvider}
 * for providing service-level authorization for Hadoop.
 */
public class ConfiguredPolicy extends Policy implements Configurable {
  public static final String HADOOP_POLICY_FILE = "hadoop-policy.xml";
  private static final Log LOG = LogFactory.getLog(ConfiguredPolicy.class);
      
  private Configuration conf;
  private PolicyProvider policyProvider;
  private volatile Map<Principal, Set<Permission>> permissions;
  private volatile Set<Permission> allowedPermissions;

  public ConfiguredPolicy(Configuration conf, PolicyProvider policyProvider) {
    this.conf = conf;      
    this.policyProvider = policyProvider;
    refresh();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    refresh();
  }

  @Override
  public boolean implies(ProtectionDomain domain, Permission permission) {
    // Only make checks for domains having principals 
    if(domain.getPrincipals().length == 0) {
      return true; 
    }

    return super.implies(domain, permission);
  }

  @Override
  public PermissionCollection getPermissions(ProtectionDomain domain) {
    PermissionCollection permissionCollection = super.getPermissions(domain);
    for (Principal principal : domain.getPrincipals()) {
      Set<Permission> principalPermissions = permissions.get(principal);
      if (principalPermissions != null) {
        for (Permission permission : principalPermissions) {
          permissionCollection.add(permission);
        }
      }

      for (Permission permission : allowedPermissions) {
        permissionCollection.add(permission);
      }
    }
    return permissionCollection;
  }

  @Override
  public void refresh() {
    // Get the system property 'hadoop.policy.file'
    String policyFile = 
      System.getProperty("hadoop.policy.file", HADOOP_POLICY_FILE);
    
    // Make a copy of the original config, and load the policy file
    Configuration policyConf = new Configuration(conf);
    policyConf.addResource(policyFile);
    
    Map<Principal, Set<Permission>> newPermissions = 
      new HashMap<Principal, Set<Permission>>();
    Set<Permission> newAllowPermissions = new HashSet<Permission>();

    // Parse the config file
    Service[] services = policyProvider.getServices();
    if (services != null) {
      for (Service service : services) {
        AccessControlList acl = 
          new AccessControlList(
              policyConf.get(service.getServiceKey(), 
                             AccessControlList.WILDCARD_ACL_VALUE)
              );
        
        if (acl.allAllowed()) {
          newAllowPermissions.add(service.getPermission());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Policy - " + service.getPermission() + " * ");
          }
        } else {
          for (String user : acl.getUsers()) {
            addPermission(newPermissions, new User(user), service.getPermission());
          }

          for (String group : acl.getGroups()) {
            addPermission(newPermissions, new Group(group), service.getPermission());
          }
        }
      }
    }

    // Flip to the newly parsed permissions
    allowedPermissions = newAllowPermissions;
    permissions = newPermissions;
  }

  private void addPermission(Map<Principal, Set<Permission>> permissions,
                             Principal principal, Permission permission) {
    Set<Permission> principalPermissions = permissions.get(principal);
    if (principalPermissions == null) {
      principalPermissions = new HashSet<Permission>();
      permissions.put(principal, principalPermissions);
    }
    principalPermissions.add(permission);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Policy - Adding  " + permission + " to " + principal);
    }
  }
}
