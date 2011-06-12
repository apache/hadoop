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

import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Permission;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An authorization manager which handles service-level authorization
 * for incoming service requests.
 */
public class ServiceAuthorizationManager {

  private static final Log LOG = 
    LogFactory.getLog(ServiceAuthorizationManager.class);
  
  /**
   * Configuration key for controlling service-level authorization for Hadoop.
   */
  public static final String SERVICE_AUTHORIZATION_CONFIG = 
    "hadoop.security.authorization";
  
  private static Map<Class<?>, Permission> protocolToPermissionMap = 
    Collections.synchronizedMap(new HashMap<Class<?>, Permission>());

  /**
   * Authorize the user to access the protocol being used.
   * 
   * @param user user accessing the service 
   * @param protocol service being accessed
   * @throws AuthorizationException on authorization failure
   */
  public static void authorize(Subject user, Class<?> protocol) 
  throws AuthorizationException {
    Permission permission = protocolToPermissionMap.get(protocol);
    if (permission == null) {
      permission = new ConnectionPermission(protocol);
      protocolToPermissionMap.put(protocol, permission);
    }
    
    checkPermission(user, permission);
  }
  
  /**
   * Check if the given {@link Subject} has all of necessary {@link Permission} 
   * set.
   * 
   * @param user <code>Subject</code> to be authorized
   * @param permissions <code>Permission</code> set
   * @throws AuthorizationException if the authorization failed
   */
  private static void checkPermission(final Subject user, 
                                      final Permission... permissions) 
  throws AuthorizationException {
    try{
      Subject.doAs(user, 
                   new PrivilegedExceptionAction<Void>() {
                     @Override
                     public Void run() throws Exception {
                       try {
                         for(Permission permission : permissions) {
                           AccessController.checkPermission(permission);
                         }
                       } catch (AccessControlException ace) {
                         LOG.info("Authorization failed for " + 
                                  UserGroupInformation.getCurrentUGI(), ace);
                         throw new AuthorizationException(ace);
                       }
                      return null;
                     }
                   }
                  );
    } catch (PrivilegedActionException e) {
      throw new AuthorizationException(e.getException());
    }
  }
  
}
