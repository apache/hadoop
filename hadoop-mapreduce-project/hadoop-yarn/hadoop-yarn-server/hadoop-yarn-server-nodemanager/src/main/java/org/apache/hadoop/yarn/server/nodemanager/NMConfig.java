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

package org.apache.hadoop.yarn.server.nodemanager;

/** this class stores all the configuration constant keys 
 * for the nodemanager. All the configuration key variables
 * that are going to be used in the nodemanager should be 
 * stored here. This allows us to see all the configuration 
 * parameters at one place.
 */
public class NMConfig {
  public static final String NM_PREFIX = "yarn.server.nodemanager.";

  public static final String DEFAULT_NM_BIND_ADDRESS = "0.0.0.0:45454";

  /** host:port address to which to bind to **/
  public static final String NM_BIND_ADDRESS = NM_PREFIX + "address";

  public static final String DEFAULT_NM_HTTP_BIND_ADDRESS = "0.0.0.0:9999";

  /** host:port address to which webserver has to bind to **/
  public static final String NM_HTTP_BIND_ADDRESS = NM_PREFIX + "http-address";

  public static final String DEFAULT_NM_LOCALIZER_BIND_ADDRESS = "0.0.0.0:4344";

  public static final String NM_LOCALIZER_BIND_ADDRESS =
    NM_PREFIX + "localizer.address";

  public static final String NM_KEYTAB = NM_PREFIX + "keytab";

  public static final String NM_CONTAINER_EXECUTOR_CLASS = NM_PREFIX
      + "container-executor.class";

  public static final String NM_LOCAL_DIR = NM_PREFIX + "local-dir";

  public static final String DEFAULT_NM_LOCAL_DIR = "/tmp/nm-local-dir";

  public static final String NM_LOG_DIR = NM_PREFIX + "log.dir"; // TODO: Rename

  public static final String DEFAULT_NM_LOG_DIR = "/tmp/logs";

  public static final String REMOTE_USER_LOG_DIR = NM_PREFIX
      + "remote-app-log-dir";

  public static final String DEFAULT_REMOTE_APP_LOG_DIR = "/tmp/logs";

  public static final int DEFAULT_NM_VMEM_GB = 8;

  public static final String NM_VMEM_GB = NM_PREFIX + "resource.memory.gb";

  // TODO: Should this instead be dictated by RM?
  public static final String HEARTBEAT_INTERVAL = NM_PREFIX
      + "heartbeat-interval";

  public static final int DEFAULT_HEARTBEAT_INTERVAL = 1000;

  public static final String NM_MAX_DELETE_THREADS = NM_PREFIX +
    "max.delete.threads";

  public static final int DEFAULT_MAX_DELETE_THREADS = 4;

  public static final String NM_MAX_PUBLIC_FETCH_THREADS = NM_PREFIX +
    "max.public.fetch.threads";

  public static final int DEFAULT_MAX_PUBLIC_FETCH_THREADS = 4;
  
  public static final String NM_LOCALIZATION_THREADS =
    NM_PREFIX + "localiation.threads";
  
  public static final int DEFAULT_NM_LOCALIZATION_THREADS = 5;

  public static final String NM_CONTAINER_MGR_THREADS =
    NM_PREFIX + "container.manager.threads";
  
  public static final int DEFAULT_NM_CONTAINER_MGR_THREADS = 5;

  public static final String NM_TARGET_CACHE_MB =
    NM_PREFIX + "target.cache.size";

  public static final long DEFAULT_NM_TARGET_CACHE_MB = 10 * 1024;

  public static final String NM_CACHE_CLEANUP_MS =
    NM_PREFIX + "target.cache.cleanup.period.ms";

  public static final long DEFAULT_NM_CACHE_CLEANUP_MS = 10 * 60 * 1000;

}
