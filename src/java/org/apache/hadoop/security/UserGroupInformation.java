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
import java.security.AccessController;
import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/** A {@link Writable} abstract class for storing user and groups information.
 */
public abstract class UserGroupInformation implements Writable, Principal {
  public static final Log LOG = LogFactory.getLog(UserGroupInformation.class);
  private static UserGroupInformation LOGIN_UGI = null;
  
  private static final ThreadLocal<Subject> currentUser =
    new ThreadLocal<Subject>();
  
  /** @return the {@link UserGroupInformation} for the current thread */ 
  public static UserGroupInformation getCurrentUGI() {
    Subject user = getCurrentUser();
    
    if (user == null) {
      user = currentUser.get();
      if (user == null) {
        return null;
      }
    }
    
    Set<UserGroupInformation> ugiPrincipals = 
      user.getPrincipals(UserGroupInformation.class);
    
    UserGroupInformation ugi = null;
    if (ugiPrincipals != null && ugiPrincipals.size() == 1) {
      ugi = ugiPrincipals.iterator().next();
      if (ugi == null) {
        throw new RuntimeException("Cannot find _current user_ UGI in the Subject!");
      }
    } else {
      throw new RuntimeException("Cannot resolve current user from subject, " +
      		                       "which had " + ugiPrincipals.size() + 
      		                       " UGI principals!");
    }
    return ugi;
  }

  /** 
   * Set the {@link UserGroupInformation} for the current thread
   * @deprecated Use {@link #setCurrentUser(UserGroupInformation)} 
   */ 
  @Deprecated
  public static void setCurrentUGI(UserGroupInformation ugi) {
    setCurrentUser(ugi);
  }

  /**
   * Return the current user <code>Subject</code>.
   * @return the current user <code>Subject</code>
   */
  static Subject getCurrentUser() {
    return Subject.getSubject(AccessController.getContext());
  }
  
  /**
   * Set the {@link UserGroupInformation} for the current thread
   * WARNING - This method should be used only in test cases and other exceptional
   * cases!
   * @param ugi {@link UserGroupInformation} for the current thread
   */
  public static void setCurrentUser(UserGroupInformation ugi) {
    Subject user = SecurityUtil.getSubject(ugi);
    currentUser.set(user);
  }
  
  /** Get username
   * 
   * @return the user's name
   */
  public abstract String getUserName();
  
  /** Get the name of the groups that the user belong to
   * 
   * @return an array of group names
   */
  public abstract String[] getGroupNames();

  /** Login and return a UserGroupInformation object. */
  public static UserGroupInformation login(Configuration conf
      ) throws LoginException {
    if (LOGIN_UGI == null) {
      LOGIN_UGI = UnixUserGroupInformation.login(conf);
    }
    return LOGIN_UGI;
  }

  /** Read a {@link UserGroupInformation} from conf */
  public static UserGroupInformation readFrom(Configuration conf
      ) throws IOException {
    try {
      return UnixUserGroupInformation.readFromConf(conf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME);
    } catch (LoginException e) {
      throw (IOException)new IOException().initCause(e);
    }
  }
}
