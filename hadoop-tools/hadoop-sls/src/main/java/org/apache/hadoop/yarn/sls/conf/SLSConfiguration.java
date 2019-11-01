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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;

@Private
@Unstable
public class SLSConfiguration {
  // sls
  public static final String PREFIX = "yarn.sls.";
  public static final String DNS_CACHING_ENABLED = PREFIX
      + "dns.caching.enabled";
  public static final boolean DNS_CACHING_ENABLED_DEFAULT = false;
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
  public static final int NM_RESOURCE_DEFAULT = 0;
  public static final String NM_HEARTBEAT_INTERVAL_MS = NM_PREFIX
                                                  + "heartbeat.interval.ms";
  public static final int NM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
  // am
  public static final String AM_PREFIX = PREFIX + "am.";
  public static final String AM_HEARTBEAT_INTERVAL_MS = AM_PREFIX
                                                  + "heartbeat.interval.ms";
  public static final String NM_RESOURCE_UTILIZATION_RATIO = NM_PREFIX
      + "resource.utilization.ratio";
  public static final int AM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
  public static final String AM_TYPE = AM_PREFIX + "type";
  public static final String AM_TYPE_PREFIX = AM_TYPE + ".";

  public static final String AM_CONTAINER_MEMORY = AM_PREFIX +
      "container.memory";
  public static final int AM_CONTAINER_MEMORY_DEFAULT = 1024;

  public static final String AM_CONTAINER_VCORES = AM_PREFIX +
      "container.vcores";
  public static final int AM_CONTAINER_VCORES_DEFAULT = 1;

  public static final float NM_RESOURCE_UTILIZATION_RATIO_DEFAULT = -1F;

  // container
  public static final String CONTAINER_PREFIX = PREFIX + "container.";
  public static final String CONTAINER_MEMORY_MB = CONTAINER_PREFIX
          + "memory.mb";
  public static final int CONTAINER_MEMORY_MB_DEFAULT = 1024;
  public static final String CONTAINER_VCORES = CONTAINER_PREFIX + "vcores";
  public static final int CONTAINER_VCORES_DEFAULT = 1;

  public static Resource getAMContainerResource(Configuration conf) {
    return Resource.newInstance(
        conf.getLong(AM_CONTAINER_MEMORY, AM_CONTAINER_MEMORY_DEFAULT),
        conf.getInt(AM_CONTAINER_VCORES, AM_CONTAINER_VCORES_DEFAULT));
  }

  // input file

  // nodes
  public static final String NUM_NODES = "num.nodes";
  public static final String NUM_RACKS = "num.racks";

  // job
  public static final String JOB_PREFIX = "job.";
  public static final String JOB_ID = JOB_PREFIX + "id";
  public static final String JOB_START_MS = JOB_PREFIX + "start.ms";
  public static final String JOB_END_MS = JOB_PREFIX + "end.ms";
  public static final String JOB_QUEUE_NAME = JOB_PREFIX + "queue.name";
  public static final String JOB_LABEL_EXPR = JOB_PREFIX + "label.expression";
  public static final String JOB_USER = JOB_PREFIX + "user";
  public static final String JOB_COUNT = JOB_PREFIX + "count";
  public static final String JOB_TASKS = JOB_PREFIX + "tasks";
  public static final String JOB_AM_PREFIX = "am.";

  // task
  public static final String TASK_PREFIX = "container.";
  public static final String COUNT = "count";
  public static final String TASK_CONTAINER = "container.";
  public static final String TASK_HOST = TASK_CONTAINER + "host";
  public static final String TASK_START_MS = TASK_CONTAINER + "start.ms";
  public static final String TASK_END_MS = TASK_CONTAINER + "end.ms";
  public static final String DURATION_MS = "duration.ms";
  public static final String TASK_DURATION_MS = TASK_CONTAINER + DURATION_MS;
  public static final String TASK_PRIORITY = TASK_CONTAINER + "priority";
  public static final String TASK_TYPE = TASK_CONTAINER + "type";
  public static final String TASK_EXECUTION_TYPE = TASK_CONTAINER
      + "execution.type";
  public static final String TASK_ALLOCATION_ID = TASK_CONTAINER
      + "allocation.id";
  public static final String TASK_REQUEST_DELAY = TASK_CONTAINER
      + "request.delay";

}
