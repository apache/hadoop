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

package org.apache.hadoop.yarn.sls.conf;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
public class SLSConfiguration {
  // sls
  public static final String PREFIX = "yarn.sls.";
  // runner
  public static final String RUNNER_PREFIX = PREFIX + "runner.";
  public static final String RUNNER_POOL_SIZE = RUNNER_PREFIX + "pool.size";
  public static final int RUNNER_POOL_SIZE_DEFAULT = 10;
  // scheduler
  public static final String SCHEDULER_PREFIX = PREFIX + "scheduler.";
  public static final String RM_SCHEDULER = SCHEDULER_PREFIX + "class";
  // metrics
  public static final String METRICS_PREFIX = PREFIX + "metrics.";
  public static final String METRICS_SWITCH = METRICS_PREFIX + "switch"; 
  public static final String METRICS_WEB_ADDRESS_PORT = METRICS_PREFIX
                                                  + "web.address.port";
  public static final String METRICS_OUTPUT_DIR = METRICS_PREFIX + "output";
  public static final int METRICS_WEB_ADDRESS_PORT_DEFAULT = 10001;
  public static final String METRICS_TIMER_WINDOW_SIZE = METRICS_PREFIX
                                                  + "timer.window.size";
  public static final int METRICS_TIMER_WINDOW_SIZE_DEFAULT = 100;
  public static final String METRICS_RECORD_INTERVAL_MS = METRICS_PREFIX
                                                  + "record.interval.ms";
  public static final int METRICS_RECORD_INTERVAL_MS_DEFAULT = 1000;
  // nm
  public static final String NM_PREFIX = PREFIX + "nm.";
  public static final String NM_MEMORY_MB = NM_PREFIX + "memory.mb";
  public static final int NM_MEMORY_MB_DEFAULT = 10240;
  public static final String NM_VCORES = NM_PREFIX + "vcores";
  public static final int NM_VCORES_DEFAULT = 10;
  public static final String NM_HEARTBEAT_INTERVAL_MS = NM_PREFIX
                                                  + "heartbeat.interval.ms";
  public static final int NM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
  // am
  public static final String AM_PREFIX = PREFIX + "am.";
  public static final String AM_HEARTBEAT_INTERVAL_MS = AM_PREFIX
                                                  + "heartbeat.interval.ms";
  public static final int AM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
  public static final String AM_TYPE = AM_PREFIX + "type.";

  // container
  public static final String CONTAINER_PREFIX = PREFIX + "container.";
  public static final String CONTAINER_MEMORY_MB = CONTAINER_PREFIX
          + "memory.mb";
  public static final int CONTAINER_MEMORY_MB_DEFAULT = 1024;
  public static final String CONTAINER_VCORES = CONTAINER_PREFIX + "vcores";
  public static final int CONTAINER_VCORES_DEFAULT = 1;

}
