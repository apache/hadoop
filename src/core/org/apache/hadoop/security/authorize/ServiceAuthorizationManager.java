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

import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An authorization manager which handles service-level authorization
 * for incoming service requests.
 */
public class ServiceAuthorizationManager {
  private static final String HADOOP_POLICY_FILE = "hadoop-policy.xml";

  private static Map<Class<?>, AccessControlList> protocolToAcl =
    new IdentityHashMap<Class<?>, AccessControlList>();
  
  /**
   * Configuration key for controlling service-level authorization for Hadoop.
   */
  public static final String SERVICE_AUTHORIZATION_CONFIG = 
    "hadoop.security.authorization";
  
  /**
   * Authorize the user to access the protocol being used.
   * 
   * @param user user accessing the service 
   * @param protocol service being accessed
   * @throws AuthorizationException on authorization failure
   */
  public static void authorize(UserGroupInformation user, 
                               Class<?> protocol
                               ) throws AuthorizationException {
    AccessControlList acl = protocolToAcl.get(protocol);
    if (acl == null) {
      throw new AuthorizationException("Protocol " + protocol + 
                                       " is not known.");
    }
    if (!acl.isUserAllowed(user)) {
      throw new AuthorizationException("User " + user.toString() + 
                                       " is not authorized for protocol " + 
                                       protocol);
    }
  }

  public static synchronized void refresh(Configuration conf,
                                          PolicyProvider provider) {
    // Get the system property 'hadoop.policy.file'
    String policyFile = 
      System.getProperty("hadoop.policy.file", HADOOP_POLICY_FILE);
    
    // Make a copy of the original config, and load the policy file
    Configuration policyConf = new Configuration(conf);
    policyConf.addResource(policyFile);
    
    final Map<Class<?>, AccessControlList> newAcls =
      new IdentityHashMap<Class<?>, AccessControlList>();

    // Parse the config file
    Service[] services = provider.getServices();
    if (services != null) {
      for (Service service : services) {
        AccessControlList acl = 
          new AccessControlList(
              policyConf.get(service.getServiceKey(), 
                             AccessControlList.WILDCARD_ACL_VALUE)
              );
        newAcls.put(service.getProtocol(), acl);
      }
    }

    // Flip to the newly parsed permissions
    protocolToAcl = newAcls;
  }
}
