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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;

@InterfaceStability.Unstable
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "HBase", "Hive"})
public class ProxyUsers {

  public static final String CONF_HADOOP_PROXYUSER = "hadoop.proxyuser";

  private static volatile ImpersonationProvider sip ;

  /**
   * Returns an instance of ImpersonationProvider.
   * Looks up the configuration to see if there is custom class specified.
   * @param conf
   * @return ImpersonationProvider
   */
  private static ImpersonationProvider getInstance(Configuration conf) {
    Class<? extends ImpersonationProvider> clazz =
        conf.getClass(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS,
            DefaultImpersonationProvider.class, ImpersonationProvider.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

  /**
   * refresh Impersonation rules
   */
  public static void refreshSuperUserGroupsConfiguration() {
    //load server side configuration;
    refreshSuperUserGroupsConfiguration(new Configuration());
  }

  /**
   * Refreshes configuration using the specified Proxy user prefix for
   * properties.
   *
   * @param conf configuration
   * @param proxyUserPrefix proxy user configuration prefix
   */
  public static void refreshSuperUserGroupsConfiguration(Configuration conf,
      String proxyUserPrefix) {
    Preconditions.checkArgument(proxyUserPrefix != null && 
        !proxyUserPrefix.isEmpty(), "prefix cannot be NULL or empty");
    // sip is volatile. Any assignment to it as well as the object's state
    // will be visible to all the other threads. 
    ImpersonationProvider ip = getInstance(conf);
    ip.init(proxyUserPrefix);
    sip = ip;
    ProxyServers.refresh(conf);
  }

  /**
   * Refreshes configuration using the default Proxy user prefix for properties.
   * @param conf configuration
   */
  public static void refreshSuperUserGroupsConfiguration(Configuration conf) {
    refreshSuperUserGroupsConfiguration(conf, CONF_HADOOP_PROXYUSER);
  }
  
  /**
   * Authorize the superuser which is doing doAs
   * 
   * @param user ugi of the effective or proxy user which contains a real user
   * @param remoteAddress the ip address of client
   * @throws AuthorizationException
   */
  public static void authorize(UserGroupInformation user, 
      String remoteAddress) throws AuthorizationException {
    if (sip==null) {
      // In a race situation, It is possible for multiple threads to satisfy this condition.
      // The last assignment will prevail.
      refreshSuperUserGroupsConfiguration(); 
    }
    sip.authorize(user, remoteAddress);
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
  public static void authorize(UserGroupInformation user, 
      String remoteAddress, Configuration conf) throws AuthorizationException {
    authorize(user,remoteAddress);
  }
  
  @VisibleForTesting 
  public static DefaultImpersonationProvider getDefaultImpersonationProvider() {
    return ((DefaultImpersonationProvider)sip);
  }
      
}
