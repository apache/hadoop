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

package org.apache.hadoop.ozone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class contains constants for configuration keys used in Ozone.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class OzoneConfigKeys {
  public static final String DFS_CONTAINER_IPC_PORT =
      "dfs.container.ipc";
  public static final int DFS_CONTAINER_IPC_PORT_DEFAULT = 50011;
  public static final String OZONE_LOCALSTORAGE_ROOT =
      "ozone.localstorage.root";
  public static final String OZONE_LOCALSTORAGE_ROOT_DEFAULT = "/tmp/ozone";
  public static final String OZONE_ENABLED =
      "ozone.enabled";
  public static final boolean OZONE_ENABLED_DEFAULT = false;
  public static final String OZONE_HANDLER_TYPE_KEY =
      "ozone.handler.type";
  public static final String OZONE_HANDLER_TYPE_DEFAULT = "distributed";
  public static final String OZONE_TRACE_ENABLED_KEY =
      "ozone.trace.enabled";
  public static final boolean OZONE_TRACE_ENABLED_DEFAULT = false;

  public static final String OZONE_CONTAINER_METADATA_DIRS =
      "ozone.container.metadata.dirs";

  public static final String OZONE_KEY_CACHE = "ozone.key.cache.size";
  public static final int OZONE_KEY_CACHE_DEFAULT = 1024;

  public static final int OZONE_SCM_CLIENT_PORT_DEFAULT = 9860;
  public static final int OZONE_SCM_DATANODE_PORT_DEFAULT = 9861;

  public static final String OZONE_SCM_CLIENT_ADDRESS_KEY =
      "ozone.scm.client.address";
  public static final String OZONE_SCM_CLIENT_BIND_HOST_KEY =
      "ozone.scm.client.bind.host";
  public static final String OZONE_SCM_CLIENT_BIND_HOST_DEFAULT =
      "0.0.0.0";

  public static final String OZONE_SCM_DATANODE_ADDRESS_KEY =
      "ozone.scm.datanode.address";
  public static final String OZONE_SCM_DATANODE_BIND_HOST_KEY =
      "ozone.scm.datanode.bind.host";
  public static final String OZONE_SCM_DATANODE_BIND_HOST_DEFAULT =
      "0.0.0.0";

  public static final String OZONE_SCM_HANDLER_COUNT_KEY =
      "ozone.scm.handler.count.key";
  public static final int OZONE_SCM_HANDLER_COUNT_DEFAULT = 10;

  public static final String OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS =
      "ozone.scm.heartbeat.interval.seconds";
  public static final int OZONE_SCM_HEARBEAT_INTERVAL_SECONDS_DEFAULT =
      30;

  public static final String OZONE_SCM_DEADNODE_INTERVAL_MS =
      "ozone.scm.dead.node.interval.ms";
  public static final long OZONE_SCM_DEADNODE_INTERVAL_DEFAULT =
      OZONE_SCM_HEARBEAT_INTERVAL_SECONDS_DEFAULT * 1000L * 20L;

  public static final String OZONE_SCM_MAX_HB_COUNT_TO_PROCESS =
      "ozone.scm.max.hb.count.to.process";
  public static final int OZONE_SCM_MAX_HB_COUNT_TO_PROCESS_DEFAULT = 5000;

  public static final String OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS =
      "ozone.scm.heartbeat.thread.interval.ms";
  public static final long OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS_DEFAULT =
      3000;

  public static final String OZONE_SCM_STALENODE_INTERVAL_MS =
      "ozone.scm.stale.node.interval.ms";
  public static final long OZONE_SCM_STALENODE_INTERVAL_DEFAULT =
      OZONE_SCM_HEARBEAT_INTERVAL_SECONDS_DEFAULT * 1000L * 3L;

  public static final String OZONE_SCM_CONTAINER_THREADS =
      "ozone.scm.container.threads";
  public static final int OZONE_SCM_CONTAINER_THREADS_DEFAULT =
      Runtime.getRuntime().availableProcessors() * 2;

  public static final String OZONE_SCM_HEARTBEAT_RPC_TIMEOUT =
      "ozone.scm.heartbeat.rpc-timeout";
  public static final long OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT =
      100;

  /**
   * Defines how frequently we will log the missing of heartbeat to a specific
   * SCM. In the default case we will write a warning message for each 10
   * sequential heart beats that we miss to a specific SCM. This is to avoid
   * overrunning the log with lots of HB missed Log statements.
   */
  public static final String OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT =
      "ozone.scm.heartbeat.log.warn.interval.count";
  public static final int OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT =
      10;

  public static final String OZONE_CONTAINER_TASK_WAIT =
      "ozone.container.task.wait.seconds";
  public static final long OZONE_CONTAINER_TASK_WAIT_DEFAULT = 5;


  // ozone.scm.names key is a set of DNS | DNS:PORT | IP Address | IP:PORT.
  // Written as a comma separated string. e.g. scm1, scm2:8020, 7.7.7.7:7777
  //
  // If this key is not specified datanodes will not be able to find
  // SCM. The SCM membership can be dynamic, so this key should contain
  // all possible SCM names. Once the SCM leader is discovered datanodes will
  // get the right list of SCMs to heartbeat to from the leader.
  // While it is good for the datanodes to know the names of all SCM nodes,
  // it is sufficient to actually know the name of on working SCM. That SCM
  // will be able to return the information about other SCMs that are part of
  // the SCM replicated Log.
  //
  //In case of a membership change, any one of the SCM machines will be
  // able to send back a new list to the datanodes.
  public static final String OZONE_SCM_NAMES = "ozone.scm.names";

  public static final int OZONE_SCM_DEFAULT_PORT = 9862;
  // File Name and path where datanode ID is to written to.
  // if this value is not set then container startup will fail.
  public static final String OZONE_SCM_DATANODE_ID = "ozone.scm.datanode.id";



  /**
   * There is no need to instantiate this class.
   */
  private OzoneConfigKeys() {
  }
}
