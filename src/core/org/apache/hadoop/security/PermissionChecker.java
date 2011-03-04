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
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/** Perform permission checking. */
public class PermissionChecker {
  static final Log LOG = LogFactory.getLog(UserGroupInformation.class);

  public final String user;
  protected final Set<String> groups = new HashSet<String>();
  public final boolean isSuper;

  /**
   * Checks if the caller has the required permission.
   * @param owner username of the owner
   * @param supergroup supergroup that the owner belongs to
   */
  public PermissionChecker(String owner, String supergroup
      ) throws AccessControlException{
    UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
    if (LOG.isDebugEnabled()) {
      LOG.debug("ugi=" + ugi);
    }

    if (ugi != null) {
      user = ugi.getUserName();
      groups.addAll(Arrays.asList(ugi.getGroupNames()));
      isSuper = user.equals(owner) || groups.contains(supergroup);
    }
    else {
      throw new AccessControlException("ugi = null");
    }
  }

  /**
   * Check if the callers group contains the required values.
   * @param group group to check
   */
  public boolean containsGroup(String group) {return groups.contains(group);}

  /**
   * Verify if the caller has the required permission. This will result into 
   * an exception if the caller is not allowed to access the resource.
   * @param owner owner of the system
   * @param supergroup supergroup of the system
   */
  public static void checkSuperuserPrivilege(UserGroupInformation owner, 
                                             String supergroup) 
  throws AccessControlException {
    PermissionChecker checker = 
      new PermissionChecker(owner.getUserName(), supergroup);
    if (!checker.isSuper) {
      throw new AccessControlException("Access denied for user " 
          + checker.user + ". Superuser privilege is required");
    }
  }
}
