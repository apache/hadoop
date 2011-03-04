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

import java.io.IOException;
import java.security.Policy;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.security.auth.Subject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;

public class SecurityUtil {

  private static final Log LOG = LogFactory.getLog(SecurityUtil.class);
  
  static {
    // Set an empty default policy
    setPolicy(new ConfiguredPolicy(new Configuration(), 
                                   PolicyProvider.DEFAULT_POLICY_PROVIDER));
  }
  
  private static Groups GROUPS = null;
  
  /**
   * Set the global security policy for Hadoop.
   * 
   * @param policy {@link Policy} used for authorization.
   */
  public static void setPolicy(Policy policy) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting Hadoop security policy");
    }
    Policy.setPolicy(policy);
  }

  /**
   * Get the current global security policy for Hadoop.
   * @return the current {@link Policy}
   */
  public static Policy getPolicy() {
    return Policy.getPolicy();
  }
  
  /**
   * Get the {@link Groups} being used to map user-to-groups.
   * @return the <code>Groups</code> being used to map user-to-groups.
   */
  public static Groups getUserToGroupsMappingService(Configuration conf) {
    if(GROUPS == null) {
      LOG.info(" Creating new Groups object");
      GROUPS = new Groups(conf);
    }
    return GROUPS;
  }
  
  /**
   * Get the {@link Subject} for the user identified by <code>ugi</code>.
   * @param ugi user
   * @return the {@link Subject} for the user identified by <code>ugi</code>
   */
  public static Subject getSubject(UserGroupInformation ugi) {
    if (ugi == null) {
      return null;
    }
    // Number of principals = username + #groups + ugi
    Set<Principal> principals =   
      new HashSet<Principal>(ugi.getGroupNames().length+1+1);
    User userPrincipal = new User(ugi.getUserName()); 
    principals.add(userPrincipal);
    for (String group : ugi.getGroupNames()) {
      Group groupPrincipal = new Group(group);
      principals.add(groupPrincipal);
    }
    principals.add(ugi);
    Subject user = 
      new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
    
    return user;
  }
  
  /**
   * Get the {@link Subject} for the user identified by <code>userName</code>.
   * @param userName user name
   * @return the {@link Subject} for the user identified by <code>userName</code>
   * @throws IOException
   */
  public static Subject getSubject(Configuration conf, String userName) 
    throws IOException {
    if (userName == null) {
      return null;
    }
    
    Set<Principal> principals = new HashSet<Principal>();
    User userPrincipal = new User(userName); 
    principals.add(userPrincipal);
    
    // Get user's groups
    List<String> groups = getUserToGroupsMappingService(conf).getGroups(userName);
    StringBuffer sb = new StringBuffer("Groups for '" + userName + "': <");
    for (String group : groups) {
      Group groupPrincipal = new Group(group);
      principals.add(groupPrincipal);
      sb.append(group + " ");
    }
    sb.append(">");
    LOG.info(sb);
    
    // Create the ugi with the right groups
    UserGroupInformation ugi = 
      new UnixUserGroupInformation(userName, 
                                   groups.toArray(new String[groups.size()]));
    principals.add(ugi);
    Subject user = 
      new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
    
    return user;
  }
  
  /**
   * Class representing a configured access control list.
   */
  public static class AccessControlList {
    
    // Indicates an ACL string that represents access to all users
    public static final String WILDCARD_ACL_VALUE = "*";

    // Set of users who are granted access.
    private Set<String> users;
    // Set of groups which are granted access
    private Set<String> groups;
    // Whether all users are granted access.
    private boolean allAllowed;
    
    /**
     * Construct a new ACL from a String representation of the same.
     * 
     * The String is a a comma separated list of users and groups.
     * The user list comes first and is separated by a space followed 
     * by the group list. For e.g. "user1,user2 group1,group2"
     * 
     * @param aclString String representation of the ACL
     */
    public AccessControlList(String aclString) {
      users = new TreeSet<String>();
      groups = new TreeSet<String>();
      if (aclString.contains(WILDCARD_ACL_VALUE) && 
          aclString.trim().equals(WILDCARD_ACL_VALUE)) {
        allAllowed = true;
      } else {
        String[] userGroupStrings = aclString.split(" ", 2);
        
        if (userGroupStrings.length >= 1) {
          String[] usersStr = userGroupStrings[0].split(",");
          if (usersStr.length >= 1) {
            addToSet(users, usersStr);
          }
        }
        
        if (userGroupStrings.length == 2) {
          String[] groupsStr = userGroupStrings[1].split(",");
          if (groupsStr.length >= 1) {
            addToSet(groups, groupsStr);
          }
        }
      }
    }
    
    public boolean allAllowed() {
      return allAllowed;
    }
    
    public Set<String> getUsers() {
      return users;
    }
    
    public Set<String> getGroups() {
      return groups;
    }
    
    private static final void addToSet(Set<String> set, String[] strings) {
      for (String s : strings) {
        s = s.trim();
        if (s.length() > 0) {
          set.add(s);
        }
      }
    }
  }
}
