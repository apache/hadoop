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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilter;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;

/**
 * Util class for ResourceManager WebApp.
 */
public final class RMWebAppUtil {

  private static final Log LOG = LogFactory.getLog(RMWebAppUtil.class);

  /**
   * Private constructor.
   */
  private RMWebAppUtil() {
    // not called
  }

  /**
   * Helper method to setup filters and authentication for ResourceManager
   * WebServices.
   *
   * Use the customized yarn filter instead of the standard kerberos filter to
   * allow users to authenticate using delegation tokens 4 conditions need to be
   * satisfied:
   *
   * 1. security is enabled.
   *
   * 2. http auth type is set to kerberos.
   *
   * 3. "yarn.resourcemanager.webapp.use-yarn-filter" override is set to true.
   *
   * 4. hadoop.http.filter.initializers container
   * AuthenticationFilterInitializer.
   *
   * @param conf RM configuration.
   * @param rmDTSecretManager RM specific delegation token secret manager.
   **/
  public static void setupSecurityAndFilters(Configuration conf,
      RMDelegationTokenSecretManager rmDTSecretManager) {

    boolean enableCorsFilter =
        conf.getBoolean(YarnConfiguration.RM_WEBAPP_ENABLE_CORS_FILTER,
            YarnConfiguration.DEFAULT_RM_WEBAPP_ENABLE_CORS_FILTER);
    boolean useYarnAuthenticationFilter = conf.getBoolean(
        YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER,
        YarnConfiguration.DEFAULT_RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER);
    String authPrefix = "hadoop.http.authentication.";
    String authTypeKey = authPrefix + "type";
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    String actualInitializers = "";
    Class<?>[] initializersClasses = conf.getClasses(filterInitializerConfKey);

    // setup CORS
    if (enableCorsFilter) {
      conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }

    boolean hasHadoopAuthFilterInitializer = false;
    boolean hasRMAuthFilterInitializer = false;
    if (initializersClasses != null) {
      for (Class<?> initializer : initializersClasses) {
        if (initializer.getName()
            .equals(AuthenticationFilterInitializer.class.getName())) {
          hasHadoopAuthFilterInitializer = true;
        }
        if (initializer.getName()
            .equals(RMAuthenticationFilterInitializer.class.getName())) {
          hasRMAuthFilterInitializer = true;
        }
      }
      if (UserGroupInformation.isSecurityEnabled()
          && useYarnAuthenticationFilter && hasHadoopAuthFilterInitializer
          && conf.get(authTypeKey, "")
              .equals(KerberosAuthenticationHandler.TYPE)) {
        ArrayList<String> target = new ArrayList<String>();
        for (Class<?> filterInitializer : initializersClasses) {
          if (filterInitializer.getName()
              .equals(AuthenticationFilterInitializer.class.getName())) {
            if (!hasRMAuthFilterInitializer) {
              target.add(RMAuthenticationFilterInitializer.class.getName());
            }
            continue;
          }
          target.add(filterInitializer.getName());
        }
        actualInitializers = StringUtils.join(",", target);

        LOG.info("Using RM authentication filter(kerberos/delegation-token)"
            + " for RM webapp authentication");
        RMAuthenticationFilter
            .setDelegationTokenSecretManager(rmDTSecretManager);
        conf.set(filterInitializerConfKey, actualInitializers);
      }
    }

    // if security is not enabled and the default filter initializer has not
    // been set, set the initializer to include the
    // RMAuthenticationFilterInitializer which in turn will set up the simple
    // auth filter.

    String initializers = conf.get(filterInitializerConfKey);
    if (!UserGroupInformation.isSecurityEnabled()) {
      if (initializersClasses == null || initializersClasses.length == 0) {
        conf.set(filterInitializerConfKey,
            RMAuthenticationFilterInitializer.class.getName());
        conf.set(authTypeKey, "simple");
      } else if (initializers.equals(StaticUserWebFilter.class.getName())) {
        conf.set(filterInitializerConfKey,
            RMAuthenticationFilterInitializer.class.getName() + ","
                + initializers);
        conf.set(authTypeKey, "simple");
      }
    }
  }
}
