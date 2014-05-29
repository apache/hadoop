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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

public class DefaultImpersonationProvider implements ImpersonationProvider {
  private static final String CONF_HOSTS = ".hosts";
  private static final String CONF_USERS = ".users";
  private static final String CONF_GROUPS = ".groups";
  private static final String CONF_HADOOP_PROXYUSER = "hadoop.proxyuser.";
  private static final String CONF_HADOOP_PROXYUSER_RE = "hadoop\\.proxyuser\\.";
  // list of users, groups and hosts per proxyuser
  private Map<String, Collection<String>> proxyUsers = 
    new HashMap<String, Collection<String>>(); 
  private Map<String, Collection<String>> proxyGroups = 
    new HashMap<String, Collection<String>>();
  private Map<String, Collection<String>> proxyHosts = 
    new HashMap<String, Collection<String>>();
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    // get all the new keys for users
    String regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_USERS;
    Map<String,String> allMatchKeys = conf.getValByRegex(regex);
    for(Entry<String, String> entry : allMatchKeys.entrySet()) {  
      Collection<String> users = StringUtils.getTrimmedStringCollection(entry.getValue());
      proxyUsers.put(entry.getKey(), users);
    }

    // get all the new keys for groups
    regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_GROUPS;
    allMatchKeys = conf.getValByRegex(regex);
    for(Entry<String, String> entry : allMatchKeys.entrySet()) {
      Collection<String> groups = StringUtils.getTrimmedStringCollection(entry.getValue());
      proxyGroups.put(entry.getKey(), groups);
      //cache the groups. This is needed for NetGroups
      Groups.getUserToGroupsMappingService(conf).cacheGroupsAdd(
          new ArrayList<String>(groups));
    }

    // now hosts
    regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_HOSTS;
    allMatchKeys = conf.getValByRegex(regex);
    for(Entry<String, String> entry : allMatchKeys.entrySet()) {
      proxyHosts.put(entry.getKey(),
          StringUtils.getTrimmedStringCollection(entry.getValue()));
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void authorize(UserGroupInformation user, 
      String remoteAddress) throws AuthorizationException {

    if (user.getRealUser() == null) {
      return;
    }
    boolean userAuthorized = false;
    boolean ipAuthorized = false;
    UserGroupInformation superUser = user.getRealUser();

    Collection<String> allowedUsers = proxyUsers.get(
        getProxySuperuserUserConfKey(superUser.getShortUserName()));

    if (isWildcardList(allowedUsers)) {
      userAuthorized = true;
    } else if (allowedUsers != null && !allowedUsers.isEmpty()) {
      if (allowedUsers.contains(user.getShortUserName())) {
        userAuthorized = true;
      }
    }

    if (!userAuthorized){
      Collection<String> allowedUserGroups = proxyGroups.get(
          getProxySuperuserGroupConfKey(superUser.getShortUserName()));

      if (isWildcardList(allowedUserGroups)) {
        userAuthorized = true;
      } else if (allowedUserGroups != null && !allowedUserGroups.isEmpty()) {
        for (String group : user.getGroupNames()) {
          if (allowedUserGroups.contains(group)) {
            userAuthorized = true;
            break;
          }
        }
      }

      if (!userAuthorized) {
        throw new AuthorizationException("User: " + superUser.getUserName()
            + " is not allowed to impersonate " + user.getUserName());
      }
    }

    Collection<String> ipList = proxyHosts.get(
        getProxySuperuserIpConfKey(superUser.getShortUserName()));

    if (isWildcardList(ipList)) {
      ipAuthorized = true;
    } else if (ipList != null && !ipList.isEmpty()) {
      for (String allowedHost : ipList) {
        InetAddress hostAddr;
        try {
          hostAddr = InetAddress.getByName(allowedHost);
        } catch (UnknownHostException e) {
          continue;
        }
        if (hostAddr.getHostAddress().equals(remoteAddress)) {
          // Authorization is successful
          ipAuthorized = true;
        }
      }
    }
    if(!ipAuthorized) {
      throw new AuthorizationException("Unauthorized connection for super-user: "
          + superUser.getUserName() + " from IP " + remoteAddress);
    }
  }

  /**
   * Return true if the configuration specifies the special configuration value
   * "*", indicating that any group or host list is allowed to use this configuration.
   */
  private boolean isWildcardList(Collection<String> list) {
    return (list != null) &&
    (list.size() == 1) &&
    (list.contains("*"));
  }
  
  /**
   * Returns configuration key for effective usergroups allowed for a superuser
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser usergroups
   */
  public static String getProxySuperuserUserConfKey(String userName) {
    return CONF_HADOOP_PROXYUSER+userName+CONF_USERS;
  }

  /**
   * Returns configuration key for effective groups allowed for a superuser
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser groups
   */
  public static String getProxySuperuserGroupConfKey(String userName) {
    return CONF_HADOOP_PROXYUSER+userName+CONF_GROUPS;
  }

  /**
   * Return configuration key for superuser ip addresses
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser ip-addresses
   */
  public static String getProxySuperuserIpConfKey(String userName) {
    return CONF_HADOOP_PROXYUSER+userName+CONF_HOSTS;
  }

  @VisibleForTesting
  public Map<String, Collection<String>> getProxyUsers() {
    return proxyUsers;
  }

  @VisibleForTesting
  public Map<String, Collection<String>> getProxyGroups() {
    return proxyGroups;
  }

  @VisibleForTesting
  public Map<String, Collection<String>> getProxyHosts() {
    return proxyHosts;
  }
}
