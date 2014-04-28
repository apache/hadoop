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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "HBase", "Hive"})
public class ProxyUsers {

  private static final String CONF_HOSTS = ".hosts";
  public static final String CONF_GROUPS = ".groups";
  public static final String CONF_HADOOP_PROXYUSER = "hadoop.proxyuser.";
  public static final String CONF_HADOOP_PROXYUSER_RE = "hadoop\\.proxyuser\\.";
  public static final String CONF_HADOOP_PROXYSERVERS = "hadoop.proxyservers";
  
  private static boolean init = false;
  // list of groups and hosts per proxyuser
  private static Map<String, Collection<String>> proxyGroups = 
    new HashMap<String, Collection<String>>();
  private static Map<String, Collection<String>> proxyHosts = 
    new HashMap<String, Collection<String>>();
  private static Collection<String> proxyServers =
    new HashSet<String>();

  /**
   * reread the conf and get new values for "hadoop.proxyuser.*.groups/hosts"
   */
  public static void refreshSuperUserGroupsConfiguration() {
    //load server side configuration;
    refreshSuperUserGroupsConfiguration(new Configuration());
  }

  /**
   * refresh configuration
   * @param conf
   */
  public static synchronized void refreshSuperUserGroupsConfiguration(Configuration conf) {
    
    // remove all existing stuff
    proxyGroups.clear();
    proxyHosts.clear();
    proxyServers.clear();

    // get all the new keys for groups
    String regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_GROUPS;
    Map<String,String> allMatchKeys = conf.getValByRegex(regex);
    for(Entry<String, String> entry : allMatchKeys.entrySet()) {
      Collection<String> groups = StringUtils.getTrimmedStringCollection(entry.getValue());
      proxyGroups.put(entry.getKey(), groups );
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
    
    // trusted proxy servers such as http proxies
    for (String host : conf.getTrimmedStrings(CONF_HADOOP_PROXYSERVERS)) {
      InetSocketAddress addr = new InetSocketAddress(host, 0);
      if (!addr.isUnresolved()) {
        proxyServers.add(addr.getAddress().getHostAddress());
      }
    }
    init = true;
  }

  public static synchronized boolean isProxyServer(String remoteAddr) { 
    if(!init) {
      refreshSuperUserGroupsConfiguration(); 
    }
    return proxyServers.contains(remoteAddr);
  }

  /**
   * Returns configuration key for effective user groups allowed for a superuser
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser groups
   */
  public static String getProxySuperuserGroupConfKey(String userName) {
    return ProxyUsers.CONF_HADOOP_PROXYUSER+userName+ProxyUsers.CONF_GROUPS;
  }
  
  /**
   * Return configuration key for superuser ip addresses
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser ip-addresses
   */
  public static String getProxySuperuserIpConfKey(String userName) {
    return ProxyUsers.CONF_HADOOP_PROXYUSER+userName+ProxyUsers.CONF_HOSTS;
  }
  
  /**
   * Authorize the superuser which is doing doAs
   * 
   * @param user ugi of the effective or proxy user which contains a real user
   * @param remoteAddress the ip address of client
   * @throws AuthorizationException
   */
  public static synchronized void authorize(UserGroupInformation user, 
      String remoteAddress) throws AuthorizationException {

    if(!init) {
      refreshSuperUserGroupsConfiguration(); 
    }

    if (user.getRealUser() == null) {
      return;
    }
    boolean groupAuthorized = false;
    boolean ipAuthorized = false;
    UserGroupInformation superUser = user.getRealUser();

    Collection<String> allowedUserGroups = proxyGroups.get(
        getProxySuperuserGroupConfKey(superUser.getShortUserName()));
    
    if (isWildcardList(allowedUserGroups)) {
      groupAuthorized = true;
    } else if (allowedUserGroups != null && !allowedUserGroups.isEmpty()) {
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
   * This function is kept to provide backward compatibility.
   * @param user
   * @param remoteAddress
   * @param conf
   * @throws AuthorizationException
   * @deprecated use {@link #authorize(UserGroupInformation, String) instead. 
   */
  @Deprecated
  public static synchronized void authorize(UserGroupInformation user, 
      String remoteAddress, Configuration conf) throws AuthorizationException {
    authorize(user,remoteAddress);
  }

  /**
   * Return true if the configuration specifies the special configuration value
   * "*", indicating that any group or host list is allowed to use this configuration.
   */
  private static boolean isWildcardList(Collection<String> list) {
    return (list != null) &&
      (list.size() == 1) &&
      (list.contains("*"));
  }

  @VisibleForTesting
  public static Map<String, Collection<String>> getProxyGroups() {
    return proxyGroups;
  }

  @VisibleForTesting
  public static Map<String, Collection<String>> getProxyHosts() {
    return proxyHosts;
  }
}
