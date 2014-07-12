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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.MachineList;

import com.google.common.annotations.VisibleForTesting;

public class DefaultImpersonationProvider implements ImpersonationProvider {
  private static final String CONF_HOSTS = ".hosts";
  private static final String CONF_USERS = ".users";
  private static final String CONF_GROUPS = ".groups";
  private static final String CONF_HADOOP_PROXYUSER = "hadoop.proxyuser.";
  private static final String CONF_HADOOP_PROXYUSER_RE = "hadoop\\.proxyuser\\.";
  private static final String CONF_HADOOP_PROXYUSER_RE_USERS_GROUPS = 
      CONF_HADOOP_PROXYUSER_RE+"[^.]*(" + Pattern.quote(CONF_USERS) +
      "|" + Pattern.quote(CONF_GROUPS) + ")";
  private static final String CONF_HADOOP_PROXYUSER_RE_HOSTS = 
      CONF_HADOOP_PROXYUSER_RE+"[^.]*"+ Pattern.quote(CONF_HOSTS);
  // acl and list of hosts per proxyuser
  private Map<String, AccessControlList> proxyUserAcl = 
    new HashMap<String, AccessControlList>();
  private static Map<String, MachineList> proxyHosts = 
    new HashMap<String, MachineList>();
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    // get list of users and groups per proxyuser
    Map<String,String> allMatchKeys = 
        conf.getValByRegex(CONF_HADOOP_PROXYUSER_RE_USERS_GROUPS); 
    for(Entry<String, String> entry : allMatchKeys.entrySet()) {  
      String aclKey = getAclKey(entry.getKey());
      if (!proxyUserAcl.containsKey(aclKey)) {
        proxyUserAcl.put(aclKey, new AccessControlList(
            allMatchKeys.get(aclKey + CONF_USERS) ,
            allMatchKeys.get(aclKey + CONF_GROUPS)));
      }
    }

    // get hosts per proxyuser
    allMatchKeys = conf.getValByRegex(CONF_HADOOP_PROXYUSER_RE_HOSTS);
    for(Entry<String, String> entry : allMatchKeys.entrySet()) {
      proxyHosts.put(entry.getKey(),
          new MachineList(entry.getValue()));
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void authorize(UserGroupInformation user, 
      String remoteAddress) throws AuthorizationException {
    
    UserGroupInformation realUser = user.getRealUser();
    if (realUser == null) {
      return;
    }
    
    AccessControlList acl = proxyUserAcl.get(
        CONF_HADOOP_PROXYUSER+realUser.getShortUserName());
    if (acl == null || !acl.isUserAllowed(user)) {
      throw new AuthorizationException("User: " + realUser.getUserName()
          + " is not allowed to impersonate " + user.getUserName());
    }

    MachineList MachineList = proxyHosts.get(
        getProxySuperuserIpConfKey(realUser.getShortUserName()));

    if(!MachineList.includes(remoteAddress)) {
      throw new AuthorizationException("Unauthorized connection for super-user: "
          + realUser.getUserName() + " from IP " + remoteAddress);
    }
  }
  
  private String getAclKey(String key) {
    int endIndex = key.lastIndexOf(".");
    if (endIndex != -1) {
      return key.substring(0, endIndex); 
    }
    return key;
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
  public Map<String, Collection<String>> getProxyGroups() {
     Map<String,Collection<String>> proxyGroups = new HashMap<String,Collection<String>>();
     for(Entry<String, AccessControlList> entry : proxyUserAcl.entrySet()) {
       proxyGroups.put(entry.getKey() + CONF_GROUPS, entry.getValue().getGroups());
     }
     return proxyGroups;
  }

  @VisibleForTesting
  public Map<String, Collection<String>> getProxyHosts() {
    Map<String, Collection<String>> tmpProxyHosts = 
        new HashMap<String, Collection<String>>();
    for (Map.Entry<String, MachineList> proxyHostEntry :proxyHosts.entrySet()) {
      tmpProxyHosts.put(proxyHostEntry.getKey(), 
          proxyHostEntry.getValue().getCollection());
    }
    return tmpProxyHosts;
  }
}
