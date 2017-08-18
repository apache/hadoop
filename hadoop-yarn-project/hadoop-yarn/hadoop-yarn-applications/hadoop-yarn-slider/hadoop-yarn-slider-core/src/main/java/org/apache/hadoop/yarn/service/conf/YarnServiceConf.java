/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.service.conf;

import org.apache.slider.api.resource.Configuration;

public class YarnServiceConf {

  // Retry settings for the ServiceClient to talk to Service AppMaster
  public static final String CLIENT_AM_RETRY_MAX_WAIT_MS = "yarn.service.client-am.retry.max-wait-ms";
  public static final String CLIENT_AM_RETRY_MAX_INTERVAL_MS = "yarn.service.client-am.retry-interval-ms";

  // Retry settings for container failures
  public static final String CONTAINER_RETRY_MAX = "yarn.service.container-failure.retry.max";
  public static final String CONTAINER_RETRY_INTERVAL = "yarn.service.container-failure.retry-interval";

  /**
   * Get long value for the property
   * @param name name of the property
   * @param defaultValue default value of the property, if it is not defined in
   *                     userConf and systemConf.
   * @param userConf Configuration provided by client in the JSON definition
   * @param systemConf The YarnConfiguration in the system.
   * @return long value for the property
   */
  public static long getLong(String name, long defaultValue,
      Configuration userConf, org.apache.hadoop.conf.Configuration systemConf) {
    return userConf.getPropertyLong(name, systemConf.getLong(name, defaultValue));
  }

  public static int getInt(String name, int defaultValue,
      Configuration userConf, org.apache.hadoop.conf.Configuration systemConf) {
    return userConf.getPropertyInt(name, systemConf.getInt(name, defaultValue));
  }
}
