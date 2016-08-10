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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

/** 
 * Manages ResourceManager audit logs. 
 *
 * Audit log format is written as key=value pairs. Tab separated.
 */
public class RMAuditLogger {
  private static final Log LOG = LogFactory.getLog(RMAuditLogger.class);

  static enum Keys {USER, OPERATION, TARGET, RESULT, IP, PERMISSIONS,
                    DESCRIPTION, APPID, APPATTEMPTID, CONTAINERID}

  public static class AuditConstants {
    static final String SUCCESS = "SUCCESS";
    static final String FAILURE = "FAILURE";
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';

    public static final String KILL_APP_REQUEST = "Kill Application Request";
    public static final String SUBMIT_APP_REQUEST = "Submit Application Request";
    public static final String MOVE_APP_REQUEST = "Move Application Request";
    public static final String FINISH_SUCCESS_APP = "Application Finished - Succeeded";
    public static final String FINISH_FAILED_APP = "Application Finished - Failed";
    public static final String FINISH_KILLED_APP = "Application Finished - Killed";
    public static final String REGISTER_AM = "Register App Master";
    public static final String AM_ALLOCATE = "App Master Heartbeats";
    public static final String UNREGISTER_AM = "Unregister App Master";
    public static final String ALLOC_CONTAINER = "AM Allocated Container";
    public static final String RELEASE_CONTAINER = "AM Released Container";

    // Some commonly used descriptions
    public static final String UNAUTHORIZED_USER = "Unauthorized user";
    
    // For Reservation system
    public static final String SUBMIT_RESERVATION_REQUEST = "Submit Reservation Request";
    public static final String UPDATE_RESERVATION_REQUEST = "Update Reservation Request";
    public static final String DELETE_RESERVATION_REQUEST = "Delete Reservation Request";
  }

  /**
   * A helper api for creating an audit log for a successful event.
   */
  static String createSuccessLog(String user, String operation, String target,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId, InetAddress ip) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    if (ip != null) {
      add(Keys.IP, ip.getHostAddress(), b);
    }
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target ,b);
    add(Keys.RESULT, AuditConstants.SUCCESS, b);
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    if (attemptId != null) {
      add(Keys.APPATTEMPTID, attemptId.toString(), b);
    }
    if (containerId != null) {
      add(Keys.CONTAINERID, containerId.toString(), b);
    }
    return b.toString();
  }

  static String createSuccessLog(String user, String operation, String target,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId) {
    return createSuccessLog(user, operation, target, appId, attemptId,
        containerId, Server.getRemoteIp());
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed. 
   * @param appId Application Id in which operation was performed.
   * @param containerId Container Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target, 
      ApplicationId appId, ContainerId containerId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, 
          containerId));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager.
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed. 
   * @param appId Application Id in which operation was performed.
   * @param attemptId Application Attempt Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target, 
      ApplicationId appId, ApplicationAttemptId attemptId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, attemptId,
          null));
    }
  }


  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager.
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed. 
   * @param appId Application Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request. 
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed. 
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, null, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user
   *          User who made the service request to the ResourceManager.
   * @param operation
   *          Operation requested by the user.
   * @param target
   *          The target on which the operation is being performed.
   * @param appId
   *          Application Id in which operation was performed.
   * @param ip
   *          The ip address of the caller.
   *
   *          <br>
   *          <br>
   *          Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val
   *          delimiter and hence the value fields should not contains tabs
   *          ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, InetAddress ip) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          createSuccessLog(user, operation, target, appId, null, null, ip));
    }
  }

  /**
   * A helper api for creating an audit log for a failure event.
   */
  static String createFailureLog(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target ,b);
    add(Keys.RESULT, AuditConstants.FAILURE, b);
    add(Keys.DESCRIPTION, description, b);
    add(Keys.PERMISSIONS, perm, b);
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    if (attemptId != null) {
      add(Keys.APPATTEMPTID, attemptId.toString(), b);
    }
    if (containerId != null) {
      add(Keys.CONTAINERID, containerId.toString(), b);
    }
    return b.toString();
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
   * @param containerId Container Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId, 
      ContainerId containerId) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          appId, null, containerId));
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
   * @param appId ApplicationId in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId, 
      ApplicationAttemptId attemptId) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          appId, attemptId, null));
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
   * @param appId ApplicationId in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          appId, null, null));
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
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          null, null, null));
    }
  }

  /**
   * A helper api to add remote IP address
   */
  static void addRemoteIP(StringBuilder b) {
    InetAddress ip = Server.getRemoteIp();
    // ip address can be null for testcases
    if (ip != null) {
      add(Keys.IP, ip.getHostAddress(), b);
    }
  }

  /**
   * Adds the first key-val pair to the passed builder in the following format
   * key=value
   */
  static void start(Keys key, String value, StringBuilder b) {
    b.append(key.name()).append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * Appends the key-val pair to the passed builder in the following format
   * <pair-delim>key=value
   */
  static void add(Keys key, String value, StringBuilder b) {
    b.append(AuditConstants.PAIR_SEPARATOR).append(key.name())
     .append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }
}
