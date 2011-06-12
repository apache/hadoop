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

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Class representing a configured access control list.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class AccessControlList {
  
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
  
  public boolean isAllAllowed() {
    return allAllowed;
  }
  
  /**
   * Get the names of users allowed for this service.
   * @return the set of user names. the set must not be modified.
   */
  Set<String> getUsers() {
    return users;
  }
  
  /**
   * Get the names of user groups allowed for this service.
   * @return the set of group names. the set must not be modified.
   */
  Set<String> getGroups() {
    return groups;
  }

  public boolean isUserAllowed(UserGroupInformation ugi) {
    if (allAllowed || users.contains(ugi.getShortUserName())) {
      return true;
    } else {
      for(String group: ugi.getGroupNames()) {
        if (groups.contains(group)) {
          return true;
        }
      }
    }
    return false;
  }
  
  private static final void addToSet(Set<String> set, String[] strings) {
    for (String s : strings) {
      s = s.trim();
      if (s.length() > 0) {
        set.add(s);
      }
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for(String user: users) {
      if (!first) {
        sb.append(",");
      } else {
        first = false;
      }
      sb.append(user);
    }
    if (!groups.isEmpty()) {
      sb.append(" ");
    }
    first = true;
    for(String group: groups) {
      if (!first) {
        sb.append(",");
      } else {
        first = false;
      }
      sb.append(group);
    }
    return sb.toString();    
  }
}