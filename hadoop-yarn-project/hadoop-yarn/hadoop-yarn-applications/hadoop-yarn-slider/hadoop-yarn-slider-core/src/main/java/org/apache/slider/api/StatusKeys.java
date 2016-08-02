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
import static org.apache.slider.api.ResourceKeys.COMPONENT_INSTANCES;
/**
 * Contains status and statistics keys
 */
public interface StatusKeys {

  String STATISTICS_CONTAINERS_ACTIVE_REQUESTS = "containers.active.requests";
  String STATISTICS_CONTAINERS_COMPLETED = "containers.completed";
  String STATISTICS_CONTAINERS_DESIRED = "containers.desired";
  String STATISTICS_CONTAINERS_FAILED = "containers.failed";
  String STATISTICS_CONTAINERS_FAILED_RECENTLY = "containers.failed.recently";
  String STATISTICS_CONTAINERS_FAILED_NODE = "containers.failed.node";
  String STATISTICS_CONTAINERS_PREEMPTED = "containers.failed.preempted";
  String STATISTICS_CONTAINERS_LIVE = "containers.live";
  String STATISTICS_CONTAINERS_REQUESTED = "containers.requested";
  String STATISTICS_CONTAINERS_ANTI_AFFINE_PENDING = "containers.anti-affine.pending";
  String STATISTICS_CONTAINERS_STARTED = "containers.start.started";
  String STATISTICS_CONTAINERS_START_FAILED =
      "containers.start.failed";
  String STATISTICS_CONTAINERS_SURPLUS =
      "containers.surplus";
  String STATISTICS_CONTAINERS_UNKNOWN_COMPLETED =
      "containers.unknown.completed";
  /**
   * No of containers provided on AM restart
   */
  String INFO_CONTAINERS_AM_RESTART = "containers.at.am-restart";

  String INFO_CREATE_TIME_MILLIS = "create.time.millis";
  String INFO_CREATE_TIME_HUMAN = "create.time";
  String INFO_LIVE_TIME_MILLIS = "live.time.millis";
  String INFO_LIVE_TIME_HUMAN = "live.time";
  String INFO_FLEX_TIME_MILLIS = "flex.time.millis";
  String INFO_FLEX_TIME_HUMAN = "flex.time";

  String INFO_MASTER_ADDRESS = "info.master.address";

  /**
   * System time in millis when the status report was generated
   */
  String INFO_STATUS_TIME_MILLIS = "status.time.millis";

  /**
   * System time in human form when the status report was generated
   */
  String INFO_STATUS_TIME_HUMAN = "status.time";

  String INFO_AM_APP_ID = "info.am.app.id";
  String INFO_AM_ATTEMPT_ID = "info.am.attempt.id";
  String INFO_AM_CONTAINER_ID = "info.am.container.id";
  String INFO_AM_HOSTNAME = "info.am.hostname";
  String INFO_AM_RPC_PORT = "info.am.rpc.port";
  String INFO_AM_WEB_PORT = "info.am.web.port";
  String INFO_AM_WEB_URL = "info.am.web.url";
  String INFO_AM_AGENT_STATUS_PORT = "info.am.agent.status.port";
  String INFO_AM_AGENT_OPS_PORT = "info.am.agent.ops.port";
  String INFO_AM_AGENT_OPS_URL = "info.am.agent.ops.url";
  String INFO_AM_AGENT_STATUS_URL = "info.am.agent.status.url";

      /**
       * info: #of instances of a component requested: {@value}
       *
       */
  String COMPONENT_INSTANCES_ACTUAL = COMPONENT_INSTANCES + ".actual";

  /**
   * info: #of instances of a component requested: {@value}
   *
   */
  String COMPONENT_INSTANCES_REQUESTING = COMPONENT_INSTANCES + ".requesting";

  /**
   * info: #of instances of a component being released: {@value}
   *
   */
  String COMPONENT_INSTANCES_RELEASING = COMPONENT_INSTANCES + ".releasing";

  /**
   * info: #of instances of a component failed: {@value}
   *
   */
  String COMPONENT_INSTANCES_FAILED = COMPONENT_INSTANCES + ".failed";

  /**
   * info: #of instances of a component started: {@value}
   *
   */
  String COMPONENT_INSTANCES_STARTED = COMPONENT_INSTANCES + ".started";


  /**
   * info: #of instances of a component completed: {@value}
   *
   */
  String COMPONENT_INSTANCES_COMPLETED = COMPONENT_INSTANCES + ".completed";


}
