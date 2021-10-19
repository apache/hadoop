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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Container exit statuses indicating special exit circumstances.
 */
@Public
@Unstable
public class ContainerExitStatus {
  public static final int SUCCESS = 0;
  public static final int INVALID = -1000;

  /**
   * Containers killed by the framework, either due to being released by
   * the application or being 'lost' due to node failures etc.
   */
  public static final int ABORTED = -100;
  
  /**
   * When threshold number of the nodemanager-local-directories or
   * threshold number of the nodemanager-log-directories become bad.
   */
  public static final int DISKS_FAILED = -101;

  /**
   * Containers preempted by the framework.
   */
  public static final int PREEMPTED = -102;

  /**
   * Container terminated because of exceeding allocated virtual memory.
   */
  public static final int KILLED_EXCEEDED_VMEM = -103;

  /**
   * Container terminated because of exceeding allocated physical memory.
   */
  public static final int KILLED_EXCEEDED_PMEM = -104;

  /**
   * Container was terminated by stop request by the app master.
   */
  public static final int KILLED_BY_APPMASTER = -105;

  /**
   * Container was terminated by the resource manager.
   */
  public static final int KILLED_BY_RESOURCEMANAGER = -106;

  /**
   * Container was terminated after the application finished.
   */
  public static final int KILLED_AFTER_APP_COMPLETION = -107;

  /**
   * Container was terminated by the ContainerScheduler to make room
   * for another container...
   */
  public static final int KILLED_BY_CONTAINER_SCHEDULER = -108;

  /**
   * Container was terminated for generating excess log data.
   */
  public static final int KILLED_FOR_EXCESS_LOGS = -109;

}
