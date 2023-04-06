/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Manages Router audit logs.
 * Audit log format is written as key=value pairs. Tab separated.
 */
public final class RouterAuditLogger {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAuditLogger.class);

  private RouterAuditLogger() {
  }

  enum Keys {USER, OPERATION, TARGET, RESULT, IP, PERMISSIONS, DESCRIPTION, APPID, SUBCLUSTERID}

  public static class AuditConstants {
    static final String SUCCESS = "SUCCESS";
    static final String FAILURE = "FAILURE";
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';

    public static final String GET_NEW_APP = "Get New App";
    public static final String SUBMIT_NEW_APP = "Submit New App";
    public static final String FORCE_KILL_APP = "Force Kill App";
    public static final String GET_APP_REPORT = "Get Application Report";
    public static final String TARGET_CLIENT_RM_SERVICE = "RouterClientRMService";
    public static final String UNKNOWN = "UNKNOWN";
    public static final String GET_APPLICATIONS = "Get Applications";
    public static final String GET_CLUSTERMETRICS = "Get ClusterMetrics";
    public static final String GET_CLUSTERNODES = "Get ClusterNodes";
    public static final String GET_QUEUEINFO = "Get QueueInfo";
    public static final String GET_QUEUE_USER_ACLS = "Get QueueUserAcls";
    public static final String MOVE_APPLICATION_ACROSS_QUEUES = "Move ApplicationAcrossQueues";
    public static final String GET_NEW_RESERVATION = "Get NewReservation";
    public static final String SUBMIT_RESERVATION = "Submit Reservation";
    public static final String LIST_RESERVATIONS = "List Reservations";
    public static final String UPDATE_RESERVATION = "Update Reservation";
    public static final String DELETE_RESERVATION = "Delete Reservation";
    public static final String GET_NODETOLABELS = "Get NodeToLabels";
    public static final String GET_LABELSTONODES = "Get LabelsToNodes";
    public static final String GET_CLUSTERNODELABELS = "Get ClusterNodeLabels";
    public static final String GET_APPLICATION_ATTEMPT_REPORT = "Get ApplicationAttemptReport";
    public static final String GET_APPLICATION_ATTEMPTS = "Get ApplicationAttempts";
    public static final String GET_CONTAINERREPORT = "Get ContainerReport";
    public static final String GET_CONTAINERS = "Get Containers";
    public static final String GET_DELEGATIONTOKEN = "Get DelegationToken";
    public static final String RENEW_DELEGATIONTOKEN = "Renew DelegationToken";
    public static final String CANCEL_DELEGATIONTOKEN = "Cancel DelegationToken";
    public static final String FAIL_APPLICATIONATTEMPT = "Fail ApplicationAttempt";
    public static final String UPDATE_APPLICATIONPRIORITY = "Update ApplicationPriority";
    public static final String SIGNAL_TOCONTAINER = "Signal ToContainer";
    public static final String UPDATE_APPLICATIONTIMEOUTS = "Update ApplicationTimeouts";
    public static final String GET_RESOURCEPROFILES = "Get ResourceProfiles";
    public static final String GET_RESOURCEPROFILE = "Get ResourceProfile";
    public static final String GET_RESOURCETYPEINFO = "Get ResourceTypeInfo";
    public static final String GET_ATTRIBUTESTONODES = "Get AttributesToNodes";
    public static final String GET_CLUSTERNODEATTRIBUTES = "Get ClusterNodeAttributes";
    public static final String GET_NODESTOATTRIBUTES = "Get NodesToAttributes";
  }

  public static void logSuccess(String user, String operation, String target) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the Router
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param appId Application Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user         User who made the service request to the Router
   * @param operation    Operation requested by the user.
   * @param target       The target on which the operation is being performed.
   * @param appId        Application Id in which operation was performed.
   * @param subClusterId Subcluster Id in which operation is performed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, SubClusterId subClusterId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, subClusterId));
    }
  }

  /**
   * A helper api for creating an audit log for a successful event.
   */
  static String createSuccessLog(String user, String operation, String target,
      ApplicationId appId, SubClusterId subClusterID) {
    StringBuilder b =
        createStringBuilderForSuccessEvent(user, operation, target);
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    if (subClusterID != null) {
      add(Keys.SUBCLUSTERID, subClusterID.toString(), b);
    }
    return b.toString();
  }

  /**
   * A helper function for creating the common portion of a successful
   * log message.
   */
  private static StringBuilder createStringBuilderForSuccessEvent(String user,
      String operation, String target) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target, b);
    add(Keys.RESULT, AuditConstants.SUCCESS, b);
    return b;
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description Some additional information as to why the operation
   *                    failed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          createFailureLog(user, operation, perm, target, description, null,
              null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param descriptionFormat the description message format string.
   * @param args format parameter.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String descriptionFormat, Object... args) {
    if (LOG.isInfoEnabled()) {
      String description = String.format(descriptionFormat, args);
      LOG.info(createFailureLog(user, operation, perm, target, description, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description Some additional information as to why the operation
   *                    failed.
   * @param appId Application Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          createFailureLog(user, operation, perm, target, description, appId,
              null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description Some additional information as to why the operation
   *                    failed.
   * @param appId Application Id in which operation was performed.
   * @param subClusterId SubCluster Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      SubClusterId subClusterId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          createFailureLog(user, operation, perm, target, description, appId,
              subClusterId));
    }
  }

  /**
   * Create a readable and parsable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description Some additional information as to why the operation failed.
   * @param subClusterId SubCluster Id in which operation was performed.
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, SubClusterId subClusterId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createFailureLog(user, operation, perm, target, description, null,
          subClusterId));
    }
  }

  /**
   * A helper api for creating an audit log for a failure event.
   */
  static String createFailureLog(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      SubClusterId subClusterId) {
    StringBuilder b =
        createStringBuilderForFailureLog(user, operation, target, description,
            perm);
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    if (subClusterId != null) {
      add(Keys.SUBCLUSTERID, subClusterId.toString(), b);
    }
    return b.toString();
  }

  /**
   * A helper function for creating the common portion of a failure
   * log message.
   */
  private static StringBuilder createStringBuilderForFailureLog(String user,
      String operation, String target, String description, String perm) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target, b);
    add(Keys.RESULT, AuditConstants.FAILURE, b);
    add(Keys.DESCRIPTION, description, b);
    add(Keys.PERMISSIONS, perm, b);
    return b;
  }

  /**
   * Adds the first key-val pair to the passed builder in the following format
   * key=value.
   */
  static void start(Keys key, String value, StringBuilder b) {
    b.append(key.name()).append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * Appends the key-val pair to the passed builder in the following format
   * <pair-delim>key=value.
   */
  static void add(Keys key, String value, StringBuilder b) {
    b.append(AuditConstants.PAIR_SEPARATOR).append(key.name())
        .append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * A helper api to add remote IP address.
   */
  static void addRemoteIP(StringBuilder b) {
    InetAddress ip = Server.getRemoteIp();
    // ip address can be null for testcases
    if (ip != null) {
      add(Keys.IP, ip.getHostAddress(), b);
    }
  }
}