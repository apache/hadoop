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

package org.apache.slider.api;

/**
 * These are the keys valid in resource options
 *
 /*

 Container failure window.

 The window is calculated in minutes as as (days * 24 *60 + hours* 24 + minutes)

 Every interval of this period after the AM is started/restarted becomes
 the time period in which the CONTAINER_FAILURE_THRESHOLD value is calculated.
 
 After the window limit is reached, the failure counts are reset. This
 is not a sliding window/moving average policy, simply a rule such as
 "every six hours the failure count is reset"


 <pre>
 ===========================================================================
 </pre>

 */
public interface ResourceKeys {


  /**
   * #of instances of a component: {@value}
   *
  */
  String COMPONENT_INSTANCES = "yarn.component.instances";

  /**
   * Whether to use unique names for each instance of a component: {@value}
   */
  String UNIQUE_NAMES = "component.unique.names";

  /**
   *  Amount of memory to ask YARN for in MB.
   *  <i>Important:</i> this may be a hard limit on the
   *  amount of RAM that the service can use
   *  {@value}
   */
  String YARN_MEMORY = "yarn.memory";
  
  /** {@value} */
  int DEF_YARN_MEMORY = 256;
  
  /**
   * Number of cores/virtual cores to ask YARN for
   *  {@value}
   */
  String YARN_CORES = "yarn.vcores";

  /**
   * Number of disks per instance to ask YARN for
   *  {@value}
   */
  String YARN_DISKS = "yarn.disks.count-per-instance";

  /**
   * Disk size per disk to ask YARN for
   *  {@value}
   */
  String YARN_DISK_SIZE = "yarn.disk.size";

  /** {@value} */
  int DEF_YARN_CORES = 1;


  /**
   * Label expression that this container must satisfy
   *  {@value}
   */
  String YARN_LABEL_EXPRESSION = "yarn.label.expression";

  /** default label expression: */
  String DEF_YARN_LABEL_EXPRESSION = null;


  /**
   * Constant to indicate that the requirements of a YARN resource limit
   * (cores, memory, ...) should be set to the maximum allowed by
   * the queue into which the YARN container requests are placed.
   */
  String YARN_RESOURCE_MAX = "max";
  
  /**
   * Mandatory property for all roles
   * 1. this must be defined.
   * 2. this must be >= 1
   * 3. this must not match any other role priority in the cluster.
   */
  String COMPONENT_PRIORITY = "yarn.role.priority";
  
  /**
   * placement policy
   */
  String COMPONENT_PLACEMENT_POLICY = "yarn.component.placement.policy";

  /**
   * Maximum number of node failures that can be tolerated by a component on a specific node
   */
  String NODE_FAILURE_THRESHOLD =
      "yarn.node.failure.threshold";

  /**
   * maximum number of failed containers (in a single role)
   * before the cluster is deemed to have failed {@value}
   */
  String CONTAINER_FAILURE_THRESHOLD =
      "yarn.container.failure.threshold";

  /**
   * prefix for the time of the container failure reset window.
   * {@value}
   */

  String CONTAINER_FAILURE_WINDOW =
      "yarn.container.failure.window";



  int DEFAULT_CONTAINER_FAILURE_WINDOW_DAYS = 0;
  int DEFAULT_CONTAINER_FAILURE_WINDOW_HOURS = 6;
  int DEFAULT_CONTAINER_FAILURE_WINDOW_MINUTES = 0;


  /**
   * Default failure threshold: {@value}
   */
  int DEFAULT_CONTAINER_FAILURE_THRESHOLD = 5;

  /**
   * Default node failure threshold for a component instance: {@value}
   * Should to be lower than default component failure threshold to allow
   * the component to start elsewhere
   */
  int DEFAULT_NODE_FAILURE_THRESHOLD = 3;

  /**
   * Failure threshold is unlimited: {@value}
   */
  int NODE_FAILURE_THRESHOLD_UNLIMITED = -1;

  /**
   * Time in seconds to escalate placement delay
   */
  String PLACEMENT_ESCALATE_DELAY =
      "yarn.placement.escalate.seconds";

  /**
   * Time to have a strict placement policy outstanding before 
   * downgrading to a lax placement (for those components which permit that).
   * <ol>
   *   <li>For strictly placed components, there's no relaxation.</li>
   *   <li>For components with no locality, there's no need to relax</li>
   * </ol>
   * 
   */
  int DEFAULT_PLACEMENT_ESCALATE_DELAY_SECONDS = 30;

  /**
   * Log aggregation include, exclude patterns
   */
  String YARN_LOG_INCLUDE_PATTERNS = "yarn.log.include.patterns";
  String YARN_LOG_EXCLUDE_PATTERNS = "yarn.log.exclude.patterns";

  String YARN_PROFILE_NAME = "yarn.resource-profile-name";

  /**
   * Window of time where application master's failure count
   * can be reset to 0.
   */
  String YARN_RESOURCEMANAGER_AM_RETRY_COUNT_WINDOW_MS  =
      "yarn.resourcemanager.am.retry-count-window-ms";

  /**
   * The default window for Slider.
   */
  long DEFAULT_AM_RETRY_COUNT_WINDOW_MS = 300000;
}
