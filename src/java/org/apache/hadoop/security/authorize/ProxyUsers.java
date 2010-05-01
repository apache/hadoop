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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class ProxyUsers {

  /*
   * Returns configuration key for effective user groups allowed for a superuser
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser groups
   */
  public static String getProxySuperuserGroupConfKey(String userName) {
    return "hadoop.proxyuser."+userName+".users";
  }
  
  /*
   * Return configuration key for superuser ip addresses
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser ip-addresses
   */
  public static String getProxySuperuserIpConfKey(String userName) {
    return "hadoop.proxyuser."+userName+".ip-addresses";
  }
  
  /*
   * Authorize the superuser which is doing doAs
   * 
   * @param user ugi of the effective or proxy user which contains a real user
   * @param remoteAddress the ip address of client
   * @param conf configuration
   * @throws AuthorizationException
   */
  public static void authorize(UserGroupInformation user, String remoteAddress,
      Configuration conf) throws AuthorizationException {

    if (user.getRealUser() == null) {
      return;
    }
    boolean groupAuthorized = false;
    UserGroupInformation superUser = user.getRealUser();

    Collection<String> allowedUserGroups = conf
        .getStringCollection(getProxySuperuserGroupConfKey(superUser
            .getShortUserName()));
    if (!allowedUserGroups.isEmpty()) {
      for (String group : user.getGroupNames()) {
        if (allowedUserGroups.contains(group)) {
          groupAuthorized = true;
          break;
        }
      }
    }

    if (!groupAuthorized) {
      throw new AuthorizationException("User: " + superUser.getUserName()
          + " is not allowed to impersonate " + user.getUserName());
    }
    
    Collection<String> ipList = conf
        .getStringCollection(getProxySuperuserIpConfKey(superUser
            .getShortUserName()));
    if (!ipList.isEmpty()) {
      for (String allowedHost : ipList) {
        InetAddress hostAddr;
        try {
          hostAddr = InetAddress.getByName(allowedHost);
        } catch (UnknownHostException e) {
          continue;
        }
        if (hostAddr.getHostAddress().equals(remoteAddress)) {
          // Authorization is successful
          return;
        }
      }
    }
    throw new AuthorizationException("Unauthorized connection for super-user: "
        + superUser.getUserName() + " from IP " + remoteAddress);
  }
}
