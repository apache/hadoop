/*
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

package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;


public class ServiceRegistryUtils {

  /**
   * Base path for services
   */
  public static final String ZK_SERVICES = "services";

  /**
   * Base path for all Slider references
   */
  public static final String ZK_SLIDER = "slider";
  public static final String ZK_USERS = "users";
  public static final String SVC_SLIDER = "/" + ZK_SERVICES + "/" + ZK_SLIDER;
  public static final String SVC_SLIDER_USERS = SVC_SLIDER + "/" + ZK_USERS;

  /**
   * Get the registry path for an instance under the user's home node
   * @param instanceName application instance
   * @return a path to the registry location for this application instance.
   */
  public static String registryPathForInstance(String instanceName) {
    return RegistryUtils.servicePath(
        RegistryUtils.currentUser(), YarnServiceConstants.APP_TYPE, instanceName
    );
  }

  /**
 * Build the path to a cluster; exists once the cluster has come up.
 * Even before that, a ZK watcher could wait for it.
 * @param username user
 * @param clustername name of the cluster
 * @return a strin
 */
  public static String mkClusterPath(String username, String clustername) {
    return mkSliderUserPath(username) + "/" + clustername;
  }

  /**
 * Build the path to a cluster; exists once the cluster has come up.
 * Even before that, a ZK watcher could wait for it.
 * @param username user
 * @return a string
 */
  public static String mkSliderUserPath(String username) {
    return SVC_SLIDER_USERS + "/" + username;
  }
}
