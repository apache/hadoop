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

import java.security.Principal;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

/**
 * Save the full and short name of the user as a principal. This allows us to
 * have a single type that we always look for when picking up user names.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
class User implements Principal {
  private final String fullName;
  private final String shortName;
  private AuthenticationMethod authMethod = null;

  public User(String name) {
    this(name, null);
  }
  
  public User(String name, AuthenticationMethod authMethod) {
    fullName = name;
    int atIdx = name.indexOf('@');
    if (atIdx == -1) {
      shortName = name;
    } else {
      int slashIdx = name.indexOf('/');
      if (slashIdx == -1 || atIdx < slashIdx) {
        shortName = name.substring(0, atIdx);
      } else {
        shortName = name.substring(0, slashIdx);
      }
    }
    this.authMethod = authMethod;
  }

  /**
   * Get the full name of the user.
   */
  @Override
  public String getName() {
    return fullName;
  }
  
  /**
   * Get the user name up to the first '/' or '@'
   * @return the leading part of the user name
   */
  public String getShortName() {
    return shortName;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    } else {
      return ((fullName.equals(((User) o).fullName)) && (authMethod == ((User) o).authMethod));
    }
  }
  
  @Override
  public int hashCode() {
    return fullName.hashCode();
  }
  
  @Override
  public String toString() {
    return fullName;
  }

  public void setAuthenticationMethod(AuthenticationMethod authMethod) {
    this.authMethod = authMethod;
  }

  public AuthenticationMethod getAuthenticationMethod() {
    return authMethod;
  }
}
