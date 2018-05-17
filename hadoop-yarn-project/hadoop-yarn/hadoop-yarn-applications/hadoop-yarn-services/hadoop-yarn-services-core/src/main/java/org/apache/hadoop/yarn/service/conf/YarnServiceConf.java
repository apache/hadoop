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

import org.apache.hadoop.yarn.service.api.records.Configuration;

// ALL SERVICE AM PROPERTIES ADDED TO THIS FILE MUST BE DOCUMENTED
// in the yarn site yarn-service/Configurations.md file.
public class YarnServiceConf {

  private static final String YARN_SERVICE_PREFIX = "yarn.service.";

  // Retry settings for the ServiceClient to talk to Service AppMaster
  public static final String CLIENT_AM_RETRY_MAX_WAIT_MS = "yarn.service.client-am.retry.max-wait-ms";
  public static final long DEFAULT_CLIENT_AM_RETRY_MAX_WAIT_MS = 15 * 60 * 1000;
  public static final String CLIENT_AM_RETRY_MAX_INTERVAL_MS = "yarn.service.client-am.retry-interval-ms";
  public static final long DEFAULT_CLIENT_AM_RETRY_MAX_INTERVAL_MS = 2 * 1000;

  // Retry settings for container failures
  public static final String CONTAINER_RETRY_MAX = "yarn.service.container-failure.retry.max";
  public static final int DEFAULT_CONTAINER_RETRY_MAX = -1;
  public static final String CONTAINER_RETRY_INTERVAL = "yarn.service.container-failure.retry-interval-ms";
  public static final int DEFAULT_CONTAINER_RETRY_INTERVAL = 30000;
  public static final String CONTAINER_FAILURES_VALIDITY_INTERVAL =
      "yarn.service.container-failure.validity-interval-ms";
  public static final long DEFAULT_CONTAINER_FAILURES_VALIDITY_INTERVAL = -1;

  public static final String AM_RESTART_MAX = "yarn.service.am-restart.max-attempts";
  public static final int DEFAULT_AM_RESTART_MAX = 20;
  public static final String AM_RESOURCE_MEM = "yarn.service.am-resource.memory";
  public static final long DEFAULT_KEY_AM_RESOURCE_MEM = 1024;

  public static final String YARN_QUEUE = "yarn.service.queue";
  public static final String DEFAULT_YARN_QUEUE = "default";

  public static final String API_SERVER_ADDRESS = "yarn.service.api-server.address";
  public static final String DEFAULT_API_SERVER_ADDRESS = "0.0.0.0:";
  public static final int DEFAULT_API_SERVER_PORT = 9191;

  public static final String FINAL_LOG_INCLUSION_PATTERN = "yarn.service.log.include-pattern";
  public static final String FINAL_LOG_EXCLUSION_PATTERN = "yarn.service.log.exclude-pattern";

  public static final String ROLLING_LOG_INCLUSION_PATTERN = "yarn.service.rolling-log.include-pattern";
  public static final String ROLLING_LOG_EXCLUSION_PATTERN = "yarn.service.rolling-log.exclude-pattern";

  public static final String YARN_SERVICES_SYSTEM_SERVICE_DIRECTORY =
      YARN_SERVICE_PREFIX + "system-service.dir";

  /**
   * The yarn service base path:
   * Defaults to HomeDir/.yarn/
   */
  public static final String YARN_SERVICE_BASE_PATH = "yarn.service.base.path";

  /**
   * maximum number of failed containers (in a single component)
   * before the app exits
   */
  public static final String CONTAINER_FAILURE_THRESHOLD =
      "yarn.service.container-failure-per-component.threshold";
  public static final int DEFAULT_CONTAINER_FAILURE_THRESHOLD = 10;

  /**
   * Maximum number of container failures on a node before the node is blacklisted
   */
  public static final String NODE_BLACKLIST_THRESHOLD =
      "yarn.service.node-blacklist.threshold";
  public static final int DEFAULT_NODE_BLACKLIST_THRESHOLD = 3;

  /**
   * The failure count for CONTAINER_FAILURE_THRESHOLD and NODE_BLACKLIST_THRESHOLD
   * gets reset periodically, the unit is seconds.
   */
  public static final String CONTAINER_FAILURE_WINDOW =
      "yarn.service.failure-count-reset.window";
  public static final long DEFAULT_CONTAINER_FAILURE_WINDOW = 21600;

  /**
   * interval between readiness checks.
   */
  public static final String READINESS_CHECK_INTERVAL = "yarn.service.readiness-check-interval.seconds";
  public static final int DEFAULT_READINESS_CHECK_INTERVAL = 30; // seconds

  /**
   * Default readiness check enabled.
   */
  public static final String DEFAULT_READINESS_CHECK_ENABLED =
      "yarn.service.default-readiness-check.enabled";
  public static final boolean DEFAULT_READINESS_CHECK_ENABLED_DEFAULT = true;

  /**
   * JVM opts.
   */
  public static final String JVM_OPTS = "yarn.service.am.java.opts";
  public static final String DEFAULT_AM_JVM_XMX = " -Xmx768m ";

  /**
   * How long to wait until a container is considered dead.
   */
  public static final String CONTAINER_RECOVERY_TIMEOUT_MS =
      YARN_SERVICE_PREFIX + "container-recovery.timeout.ms";

  public static final int DEFAULT_CONTAINER_RECOVERY_TIMEOUT_MS = 120000;

  /**
   * The dependency tarball file location.
   */
  public static final String DEPENDENCY_TARBALL_PATH = YARN_SERVICE_PREFIX
      + "framework.path";

  public static final String YARN_SERVICE_CONTAINER_HEALTH_THRESHOLD_PREFIX =
      YARN_SERVICE_PREFIX + "container-health-threshold.";

  /**
   * Upgrade feature enabled for services.
   */
  public static final String YARN_SERVICE_UPGRADE_ENABLED =
      "yarn.service.upgrade.enabled";
  public static final boolean YARN_SERVICE_UPGRADE_ENABLED_DEFAULT = false;

  /**
   * The container health threshold percent when explicitly set for a specific
   * component or globally for all components, will schedule a health check
   * monitor to periodically check for the percentage of healthy containers. It
   * runs the check at a specified/default poll frequency. It allows a component
   * to be below the health threshold for a specified/default window after which
   * it considers the service to be unhealthy and triggers a service stop. When
   * health threshold percent is enabled, CONTAINER_FAILURE_THRESHOLD is
   * ignored.
   */
  public static final String CONTAINER_HEALTH_THRESHOLD_PERCENT =
      YARN_SERVICE_CONTAINER_HEALTH_THRESHOLD_PREFIX + "percent";
  /**
   * Health check monitor poll frequency. It is an advanced setting and does not
   * need to be set unless the service owner understands the implication and
   * does not want the default.
   */
  public static final String CONTAINER_HEALTH_THRESHOLD_POLL_FREQUENCY_SEC =
      YARN_SERVICE_CONTAINER_HEALTH_THRESHOLD_PREFIX + "poll-frequency-secs";
  /**
   * The amount of time the health check monitor allows a specific component to
   * be below the health threshold after which it considers the service to be
   * unhealthy.
   */
  public static final String CONTAINER_HEALTH_THRESHOLD_WINDOW_SEC =
      YARN_SERVICE_CONTAINER_HEALTH_THRESHOLD_PREFIX + "window-secs";
  /**
   * The amount of initial time the health check monitor waits before the first
   * check kicks in. It gives a lead time for the service containers to come up
   * for the first time.
   */
  public static final String CONTAINER_HEALTH_THRESHOLD_INIT_DELAY_SEC =
      YARN_SERVICE_CONTAINER_HEALTH_THRESHOLD_PREFIX + "init-delay-secs";
  /**
   * By default the health threshold percent does not come into play until it is
   * explicitly set in resource config for a specific component or globally for
   * all components. -1 signifies disabled.
   */
  public static final int CONTAINER_HEALTH_THRESHOLD_PERCENT_DISABLED = -1;

  public static final int DEFAULT_CONTAINER_HEALTH_THRESHOLD_PERCENT =
      CONTAINER_HEALTH_THRESHOLD_PERCENT_DISABLED;
  public static final long DEFAULT_CONTAINER_HEALTH_THRESHOLD_POLL_FREQUENCY_SEC = 10;
  public static final long DEFAULT_CONTAINER_HEALTH_THRESHOLD_WINDOW_SEC = 600;
  // The default for initial delay is same as default health window
  public static final long DEFAULT_CONTAINER_HEALTH_THRESHOLD_INIT_DELAY_SEC =
      DEFAULT_CONTAINER_HEALTH_THRESHOLD_WINDOW_SEC;

  /**
   * Get long value for the property. First get from the userConf, if not
   * present, get from systemConf.
   *
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

  public static boolean getBoolean(String name, boolean defaultValue,
      Configuration userConf, org.apache.hadoop.conf.Configuration systemConf) {
    return userConf.getPropertyBool(name, systemConf.getBoolean(name,
        defaultValue));
  }

  public static String get(String name, String defaultVal,
      Configuration userConf, org.apache.hadoop.conf.Configuration systemConf) {
    return userConf.getProperty(name, systemConf.get(name, defaultVal));
  }
}
