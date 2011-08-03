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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class RMConfig {
  public static final String RM_KEYTAB = YarnConfiguration.RM_PREFIX
      + "keytab";
  public static final String ZK_ADDRESS = YarnConfiguration.RM_PREFIX
      + "zookeeper.address";
  public static final String ZK_SESSION_TIMEOUT = YarnConfiguration.RM_PREFIX
      + "zookeeper.session.timeout";
  public static final String ADMIN_ADDRESS = YarnConfiguration.RM_PREFIX
      + "admin.address";
  public static final String AM_MAX_RETRIES = YarnConfiguration.RM_PREFIX
      + "application.max.retries";
  public static final int DEFAULT_ZK_TIMEOUT = 60000;
  public static final int DEFAULT_AM_MAX_RETRIES = 3;
  public static final int DEFAULT_AM_EXPIRY_INTERVAL = 60000;
  public static final String NM_EXPIRY_INTERVAL = YarnConfiguration.RM_PREFIX
      + "nodemanager.expiry.interval";
  public static final int DEFAULT_NM_EXPIRY_INTERVAL = 600000;
  public static final String DEFAULT_ADMIN_BIND_ADDRESS = "0.0.0.0:8141";
  public static final String RESOURCE_SCHEDULER = YarnConfiguration.RM_PREFIX
      + "scheduler";
  public static final String RM_STORE = YarnConfiguration.RM_PREFIX + "store";
  public static final String AMLIVELINESS_MONITORING_INTERVAL =
      YarnConfiguration.RM_PREFIX
          + "amliveliness-monitor.monitoring-interval";
  public static final int DEFAULT_AMLIVELINESS_MONITORING_INTERVAL = 1000;
  public static final String CONTAINER_LIVELINESS_MONITORING_INTERVAL
    = YarnConfiguration.RM_PREFIX
      + "amliveliness-monitor.monitoring-interval";
  public static final int DEFAULT_CONTAINER_LIVELINESS_MONITORING_INTERVAL = 600000;
  public static final String NMLIVELINESS_MONITORING_INTERVAL =
      YarnConfiguration.RM_PREFIX
          + "nmliveliness-monitor.monitoring-interval";
  public static final int DEFAULT_NMLIVELINESS_MONITORING_INTERVAL = 1000;
  
  public static final String RM_RESOURCE_TRACKER_THREADS =
    YarnConfiguration.RM_PREFIX + "resource.tracker.threads";
  public static final int DEFAULT_RM_RESOURCE_TRACKER_THREADS = 10;
  
  public static final String RM_CLIENT_THREADS =
    YarnConfiguration.RM_PREFIX + "client.threads";
  public static final int DEFAULT_RM_CLIENT_THREADS = 10;
  
  public static final String RM_AM_THREADS =
    YarnConfiguration.RM_PREFIX + "am.threads";
  public static final int DEFAULT_RM_AM_THREADS = 10;
  
  public static final String RM_ADMIN_THREADS =
    YarnConfiguration.RM_PREFIX + "admin.threads";
  public static final int DEFAULT_RM_ADMIN_THREADS = 1;

  /* key for looking up the acls configuration for acls checking for application */
  public static final String RM_ACLS_ENABLED = YarnConfiguration.RM_PREFIX +
    "acls.enabled";
  
  public static String RM_ADMIN_ACL = 
    YarnConfiguration.RM_PREFIX + "admin.acl";
  public static String DEFAULT_RM_ADMIN_ACL = "*"; 

  public static String RM_NODES_INCLUDE_FILE = 
    YarnConfiguration.RM_PREFIX + "nodes.include";
  public static String DEFAULT_RM_NODES_INCLUDE_FILE = "";
  
  public static String RM_NODES_EXCLUDE_FILE = 
    YarnConfiguration.RM_PREFIX + "nodes.exclude";
  public static String DEFAULT_RM_NODES_EXCLUDE_FILE = "";
}
