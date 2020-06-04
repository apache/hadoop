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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

/**
 * Constants for {@code RMWebServiceProtocol}.
 */
public final class RMWSConsts {

  public static final String EMPTY = "";
  public static final String ANY = "*";

  public static final String FORWARDED_FOR = "X-Forwarded-For";

  // ----------------Paths for RMWebServiceProtocol----------------

  /** Path for {@code RMWebServiceProtocol}. */
  public static final String RM_WEB_SERVICE_PATH = "/ws/v1/cluster";

  /** Path for {@code RMWebServiceProtocol#getClusterInfo}. */
  public static final String INFO = "/info";

  /** Path for {@code RMWebServiceProtocol#getClusterUserInfo}. */
  public static final String CLUSTER_USER_INFO = "/userinfo";

  /** Path for {@code RMWebServiceProtocol#getClusterMetricsInfo}. */
  public static final String METRICS = "/metrics";

  /** Path for {@code RMWebServiceProtocol#getSchedulerInfo}. */
  public static final String SCHEDULER = "/scheduler";

  /** Path for {@code RMWebServices#updateSchedulerConfiguration}. */
  public static final String SCHEDULER_CONF = "/scheduler-conf";

  /** Path for {@code RMWebServices#formatSchedulerConfiguration}. */
  public static final String FORMAT_SCHEDULER_CONF = "/scheduler-conf/format";

  /** Path for {@code RMWebServices#getSchedulerConfigurationVersion}. */
  public static final String SCHEDULER_CONF_VERSION = "/scheduler-conf/version";

  /** Path for {@code RMWebServiceProtocol#dumpSchedulerLogs}. */
  public static final String SCHEDULER_LOGS = "/scheduler/logs";

  /**
   * Path for {@code RMWebServiceProtocol#validateAndGetSchedulerConfiguration}.
   */
  public static final String SCHEDULER_CONF_VALIDATE
          = "/scheduler-conf/validate";

  /** Path for {@code RMWebServiceProtocol#getNodes}. */
  public static final String NODES = "/nodes";

  /** Path for {@code RMWebServiceProtocol#getNode}. */
  public static final String NODES_NODEID = "/nodes/{nodeId}";

  /** Path for {@code RMWebServiceProtocol#updateNodeResource}. */
  public static final String NODE_RESOURCE = "/nodes/{nodeId}/resource";

  /**
   * Path for {@code RMWebServiceProtocol#getApps} and
   * {@code RMWebServiceProtocol#getApp}.
   */
  public static final String APPS = "/apps";

  /** Path for {@code RMWebServiceProtocol#getActivities}. */
  public static final String SCHEDULER_ACTIVITIES = "/scheduler/activities";

  /** Path for {@code RMWebServiceProtocol#getAppActivities}. */
  public static final String SCHEDULER_APP_ACTIVITIES =
      "/scheduler/app-activities/{appid}";

  /** Path for {@code RMWebServiceProtocol#getAppStatistics}. */
  public static final String APP_STATISTICS = "/appstatistics";

  /** Path for {@code RMWebServiceProtocol#getApp}. */
  public static final String APPS_APPID = "/apps/{appid}";

  /** Path for {@code RMWebServiceProtocol#getAppAttempts}. */
  public static final String APPS_APPID_APPATTEMPTS =
      "/apps/{appid}/appattempts";

  /** Path for {@code WebServices#getAppAttempt}. */
  public static final String APPS_APPID_APPATTEMPTS_APPATTEMPTID =
      "/apps/{appid}/appattempts/{appattemptid}";

  /** Path for {@code WebServices#getContainers}. */
  public static final String APPS_APPID_APPATTEMPTS_APPATTEMPTID_CONTAINERS =
      "/apps/{appid}/appattempts/{appattemptid}/containers";

  /** Path for {@code RMWebServiceProtocol#getNodeToLabels}. */
  public static final String GET_NODE_TO_LABELS = "/get-node-to-labels";

  /** Path for {@code RMWebServiceProtocol#getLabelsToNodes}. */
  public static final String LABEL_MAPPINGS = "/label-mappings";

  /** Path for {@code RMWebServiceProtocol#replaceLabelsOnNodes}. */
  public static final String REPLACE_NODE_TO_LABELS = "/replace-node-to-labels";

  /** Path for {@code RMWebServiceProtocol#replaceLabelsOnNode}. */
  public static final String NODES_NODEID_REPLACE_LABELS =
      "/nodes/{nodeId}/replace-labels";

  /** Path for {@code RMWebServiceProtocol#getClusterNodeLabels}. */
  public static final String GET_NODE_LABELS = "/get-node-labels";

  /** Path for {@code RMWebServiceProtocol#addToClusterNodeLabels}. */
  public static final String ADD_NODE_LABELS = "/add-node-labels";

  /** Path for {@code RMWebServiceProtocol#removeFromCluserNodeLabels}. */
  public static final String REMOVE_NODE_LABELS = "/remove-node-labels";

  /** Path for {@code RMWebServiceProtocol#getLabelsOnNode}. */
  public static final String NODES_NODEID_GETLABELS =
      "/nodes/{nodeId}/get-labels";

  /**
   * Path for {@code RMWebServiceProtocol#getAppPriority} and
   * {@code RMWebServiceProtocol#updateApplicationPriority}.
   */
  public static final String APPS_APPID_PRIORITY = "/apps/{appid}/priority";

  /**
   * Path for {@code RMWebServiceProtocol#getAppQueue} and
   * {@code RMWebServiceProtocol#updateAppQueue}.
   */
  public static final String APPS_APPID_QUEUE = "/apps/{appid}/queue";

  /** Path for {@code RMWebServiceProtocol#createNewApplication}. */
  public static final String APPS_NEW_APPLICATION = "/apps/new-application";

  /**
   * Path for {@code RMWebServiceProtocol#getAppState} and
   * {@code RMWebServiceProtocol#updateAppState}.
   */
  public static final String APPS_APPID_STATE = "/apps/{appid}/state";

  /**
   * Path for {@code RMWebServiceProtocol#postDelegationToken} and
   * {@code RMWebServiceProtocol#cancelDelegationToken}.
   */
  public static final String DELEGATION_TOKEN = "/delegation-token";

  /** Path for {@code RMWebServiceProtocol#postDelegationTokenExpiration}. */
  public static final String DELEGATION_TOKEN_EXPIRATION =
      "/delegation-token/expiration";

  /** Path for {@code RMWebServiceProtocol#createNewReservation}. */
  public static final String RESERVATION_NEW = "/reservation/new-reservation";

  /** Path for {@code RMWebServiceProtocol#submitReservation}. */
  public static final String RESERVATION_SUBMIT = "/reservation/submit";

  /** Path for {@code RMWebServiceProtocol#updateReservation}. */
  public static final String RESERVATION_UPDATE = "/reservation/update";

  /** Path for {@code RMWebServiceProtocol#deleteReservation}. */
  public static final String RESERVATION_DELETE = "/reservation/delete";

  /** Path for {@code RMWebServiceProtocol#listReservation}. */
  public static final String RESERVATION_LIST = "/reservation/list";

  /** Path for {@code RMWebServiceProtocol#getAppTimeout}. */
  public static final String APPS_TIMEOUTS_TYPE =
      "/apps/{appid}/timeouts/{type}";

  /**
   * Path for {@code RMWebServiceProtocol#getAppTimeouts}.
   */
  public static final String APPS_TIMEOUTS = "/apps/{appid}/timeouts";

  /**
   * Path for {@code RMWebServiceProtocol#updateApplicationTimeout}.
   */
  public static final String APPS_TIMEOUT = "/apps/{appid}/timeout";

  /**
   * Path for {@code RouterWebServices#getContainer}.
   */
  public static final String GET_CONTAINER =
      "/apps/{appid}/appattempts/{appattemptid}/containers/{containerid}";

  /**
   * Path for {code checkUserAccessToQueue#}
   */
  public static final String CHECK_USER_ACCESS_TO_QUEUE =
      "/queues/{queue}/access";

  /**
   * Path for {@code RMWebServiceProtocol#signalContainer}.
   */
  public static final String SIGNAL_TO_CONTAINER =
      "/containers/{containerid}/signal/{command}";

  // ----------------QueryParams for RMWebServiceProtocol----------------

  public static final String TIME = "time";
  public static final String STATES = "states";
  public static final String NODEID = "nodeId";
  public static final String STATE = "state";
  public static final String FINAL_STATUS = "finalStatus";
  public static final String USER = "user";
  public static final String QUEUE = "queue";
  public static final String QUEUES = "queues";
  public static final String LIMIT = "limit";
  public static final String STARTED_TIME_BEGIN = "startedTimeBegin";
  public static final String STARTED_TIME_END = "startedTimeEnd";
  public static final String FINISHED_TIME_BEGIN = "finishedTimeBegin";
  public static final String FINISHED_TIME_END = "finishedTimeEnd";
  public static final String APPLICATION_TYPES = "applicationTypes";
  public static final String APPLICATION_TAGS = "applicationTags";
  public static final String APP_ID = "appId";
  public static final String MAX_TIME = "maxTime";
  public static final String APPATTEMPTID = "appattemptid";
  public static final String APPID = "appid";
  public static final String LABELS = "labels";
  public static final String RESERVATION_ID = "reservation-id";
  public static final String START_TIME = "start-time";
  public static final String END_TIME = "end-time";
  public static final String INCLUDE_RESOURCE = "include-resource-allocations";
  public static final String TYPE = "type";
  public static final String CONTAINERID = "containerid";
  public static final String APPATTEMPTS = "appattempts";
  public static final String TIMEOUTS = "timeouts";
  public static final String PRIORITY = "priority";
  public static final String TIMEOUT = "timeout";
  public static final String ATTEMPTS = "appattempts";
  public static final String GET_LABELS = "get-labels";
  public static final String DESELECTS = "deSelects";
  public static final String CONTAINERS = "containers";
  public static final String QUEUE_ACL_TYPE = "queue-acl-type";
  public static final String REQUEST_PRIORITIES = "requestPriorities";
  public static final String ALLOCATION_REQUEST_IDS = "allocationRequestIds";
  public static final String GROUP_BY = "groupBy";
  public static final String SIGNAL = "signal";
  public static final String COMMAND = "command";
  public static final String ACTIONS = "actions";
  public static final String SUMMARIZE = "summarize";
  public static final String NAME = "name";

  private RMWSConsts() {
    // not called
  }

  /**
   * Defines the groupBy types of activities, currently only support
   * DIAGNOSTIC with which user can query aggregated activities
   * grouped by allocation state and diagnostic.
   */
  public enum ActivitiesGroupBy {
    DIAGNOSTIC
  }

  /**
   * Defines the required action of app activities:
   * REFRESH means to turn on activities recording for the required app,
   * GET means the required app activities should be involved in response.
   */
  public enum AppActivitiesRequiredAction {
    REFRESH, GET
  }
}