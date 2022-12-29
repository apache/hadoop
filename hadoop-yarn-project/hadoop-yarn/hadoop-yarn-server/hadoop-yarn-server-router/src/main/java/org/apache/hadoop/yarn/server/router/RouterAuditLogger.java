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
public class RouterAuditLogger {
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