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

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

/** 
 * Manages ResourceManager audit logs. 
 *
 * Audit log format is written as key=value pairs. Tab separated.
 */
public class RMAuditLogger {
  private static final Log LOG = LogFactory.getLog(RMAuditLogger.class);

  enum Keys {USER, OPERATION, TARGET, RESULT, IP, PERMISSIONS,
                    DESCRIPTION, APPID, APPATTEMPTID, CONTAINERID, 
                    CALLERCONTEXT, CALLERSIGNATURE, RESOURCE, QUEUENAME,
                    INCLUDEAPPS, INCLUDECHILDQUEUES, RECURSIVE, NODELABEL}

  public static class AuditConstants {
    static final String SUCCESS = "SUCCESS";
    static final String FAILURE = "FAILURE";
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';

    public static final String FAIL_ATTEMPT_REQUEST = "Fail Attempt Request";
    public static final String KILL_APP_REQUEST = "Kill Application Request";
    public static final String SUBMIT_APP_REQUEST = "Submit Application Request";
    public static final String MOVE_APP_REQUEST = "Move Application Request";
    public static final String GET_APP_STATE = "Get Application State";
    public static final String GET_APP_PRIORITY = "Get Application Priority";
    public static final String GET_APP_QUEUE = "Get Application Queue";
    public static final String GET_APP_ATTEMPTS = "Get Application Attempts";
    public static final String GET_APP_ATTEMPT_REPORT
        = "Get Application Attempt Report";
    public static final String GET_CONTAINERS = "Get Containers";
    public static final String GET_CONTAINER_REPORT = "Get Container Report";
    public static final String GET_QUEUE_INFO_REQUEST =
        "Get Queue Info Request";
    public static final String GET_APPLICATIONS_REQUEST =
        "Get Applications Request";
    public static final String FINISH_SUCCESS_APP = "Application Finished - Succeeded";
    public static final String FINISH_FAILED_APP = "Application Finished - Failed";
    public static final String FINISH_KILLED_APP = "Application Finished - Killed";
    public static final String REGISTER_AM = "Register App Master";
    public static final String AM_ALLOCATE = "App Master Heartbeats";
    public static final String UNREGISTER_AM = "Unregister App Master";
    public static final String ALLOC_CONTAINER = "AM Allocated Container";
    public static final String RELEASE_CONTAINER = "AM Released Container";
    public static final String UPDATE_APP_PRIORITY =
        "Update Application Priority";
    public static final String UPDATE_APP_TIMEOUTS =
        "Update Application Timeouts";
    public static final String GET_APP_TIMEOUTS = "Get Application Timeouts";
    public static final String CHANGE_CONTAINER_RESOURCE =
        "AM Changed Container Resource";
    public static final String SIGNAL_CONTAINER = "Signal Container Request";

    // Some commonly used descriptions
    public static final String UNAUTHORIZED_USER = "Unauthorized user";
    
    // For Reservation system
    public static final String CREATE_NEW_RESERVATION_REQUEST = "Create " +
        "Reservation Request";
    public static final String SUBMIT_RESERVATION_REQUEST = "Submit Reservation Request";
    public static final String UPDATE_RESERVATION_REQUEST = "Update Reservation Request";
    public static final String DELETE_RESERVATION_REQUEST = "Delete Reservation Request";
    public static final String LIST_RESERVATION_REQUEST = "List " +
            "Reservation Request";
  }

  static String createSuccessLog(String user, String operation, String target,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId, Resource resource) {
    return createSuccessLog(user, operation, target, appId, attemptId,
        containerId, resource, null, Server.getRemoteIp(), null, null);
  }

  /**
   * A helper function for creating the common portion of a successful
   * log message.
   */
  private static StringBuilder createStringBuilderForSuccessEvent(String user,
      String operation, String target, InetAddress ip) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    if (ip != null) {
      add(Keys.IP, ip.getHostAddress(), b);
    }
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target ,b);
    add(Keys.RESULT, AuditConstants.SUCCESS, b);
    return b;
  }

  /**
   * A helper api for creating an audit log for a successful event.
   */
  static String createSuccessLog(String user, String operation, String target,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId, Resource resource, CallerContext callerContext,
      InetAddress ip, String queueName, String partition) {
    StringBuilder b =
        createStringBuilderForSuccessEvent(user, operation, target, ip);
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    if (attemptId != null) {
      add(Keys.APPATTEMPTID, attemptId.toString(), b);
    }
    if (containerId != null) {
      add(Keys.CONTAINERID, containerId.toString(), b);
    }
    if (resource != null) {
      add(Keys.RESOURCE, resource.toString(), b);
    }
    appendCallerContext(b, callerContext);
    if (queueName != null) {
      add(Keys.QUEUENAME, queueName, b);
    }
    if (partition != null) {
      add(Keys.NODELABEL, partition, b);
    }
    return b.toString();
  }
  
  private static void appendCallerContext(StringBuilder sb, CallerContext callerContext) {
    String context = null;
    byte[] signature = null;
    
    if (callerContext != null) {
      context = callerContext.getContext();
      signature = callerContext.getSignature();
    }
    
    if (context != null) {
      add(Keys.CALLERCONTEXT, context, sb);
    }
    
    if (signature != null) {
      try {
        String sigStr = new String(signature, "UTF-8");
        add(Keys.CALLERSIGNATURE, sigStr, sb);
      } catch (UnsupportedEncodingException e) {
        // ignore this signature
      }
    }
  }

  /**
   * A general helper api for creating an audit log for a successful event.
   */
  @SuppressWarnings("rawtypes")
  static String createSuccessLog(String user, String operation, String target,
      InetAddress ip, ArgsBuilder args) {
    StringBuilder b =
        createStringBuilderForSuccessEvent(user, operation, target, ip);
    if(args != null) {
      add(args, b);
    }
    return b.toString();
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed. 
   * @param appId Application Id in which operation was performed.
   * @param containerId Container Id in which operation was performed.
   * @param resource Resource associated with container.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target, 
      ApplicationId appId, ContainerId containerId, Resource resource) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, 
          containerId, resource));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param appId Application Id in which operation was performed.
   * @param containerId Container Id in which operation was performed.
   * @param resource Resource associated with container.
   * @param queueName Name of queue.
   * @param partition Name of labeled partition.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, ContainerId containerId, Resource resource,
      String queueName, String partition) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null,
          containerId, resource, null, Server.getRemoteIp(), queueName,
          partition));
    }
  }

  /**
   * Create a general readable and parseable audit log string for a successful
   * event.
   *
   * @param user User who made the service request to the ResourceManager.
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param args The ArgsBuilder arguments for the operation request.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   * <br>
   * This method will attempt to retrieve the remote IP
   */
  public static void logSuccess(String user, String operation, String target,
      ArgsBuilder args) {
    logSuccess(user, operation, target, Server.getRemoteIp(), args);
  }

  /**
   * Create a general readable and parseable audit log string for a successful
   * event.
   *
   * @param user User who made the service request to the ResourceManager.
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param ip The ip address of the caller.
   * @param args The ArgsBuilder arguments for the operation request.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      InetAddress ip, ArgsBuilder args) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, ip, args));
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
          null, null));
    }
  }

  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, CallerContext callerContext) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, null,
          null, callerContext, Server.getRemoteIp(), null, null));
    }
  }

  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, CallerContext callerContext, String queueName) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, null,
          null, callerContext, Server.getRemoteIp(), queueName, null));
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
      LOG.info(createSuccessLog(user, operation, target, appId, null, null,
          null, null, ip, null, null));
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
      LOG.info(createSuccessLog(user, operation, target, appId, null, null, null));
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
      LOG.info(createSuccessLog(user, operation, target, null, null, null, null));
    }
  }
  
  private static StringBuilder createStringBuilderForFailureLog(String user,
      String operation, String target, String description, String perm) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target ,b);
    add(Keys.RESULT, AuditConstants.FAILURE, b);
    add(Keys.DESCRIPTION, description, b);
    add(Keys.PERMISSIONS, perm, b);
    return b;
  }

  /**
   * A helper api for creating an audit log for a failure event.
   */
  static String createFailureLog(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId,
      Resource resource, CallerContext callerContext, String queueName) {
    StringBuilder b = createStringBuilderForFailureLog(user,
        operation, target, description, perm);
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    if (attemptId != null) {
      add(Keys.APPATTEMPTID, attemptId.toString(), b);
    }
    if (containerId != null) {
      add(Keys.CONTAINERID, containerId.toString(), b);
    }
    if (resource != null) {
      add(Keys.RESOURCE, resource.toString(), b);
    }
    appendCallerContext(b, callerContext);
    if (queueName != null) {
      add(Keys.QUEUENAME, queueName, b);
    }
    return b.toString();
  }

  /**
   * A helper api for creating an audit log for a failure event.
   */
  static String createFailureLog(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId, Resource resource) {
    return createFailureLog(user, operation, perm, target, description, appId,
        attemptId, containerId, resource, null, null);
  }

  /**
   * A helper api for creating an audit log for a failure event.
   */
  @SuppressWarnings("rawtypes")
  static String createFailureLog(String user, String operation, String perm,
      String target, String description, ArgsBuilder args) {
    StringBuilder b = createStringBuilderForFailureLog(user,
        operation, target, description, perm);
    if(args != null) {
      add(args, b);
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
   * @param resource Resources associated with container.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId, 
      ContainerId containerId, Resource resource) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          appId, null, containerId, resource));
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
          appId, attemptId, null, null));
    }
  }

  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      CallerContext callerContext) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          appId, null, null, null, callerContext, null));
    }
  }

  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      CallerContext callerContext, String queueName) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          appId, null, null, null, callerContext, queueName));
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
          appId, null, null, null));
    }
  }

  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      String queueName) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          appId, null, null, null, null, queueName));
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
          null, null, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description The failure description
   * @param args The arguments for the operation request.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ArgsBuilder args) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description,
          args));
    }
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

  /**
   * Appends the key-val pair to the passed builder in the following format
   * <pair-delim>key=value
   */
  static void add(ArgsBuilder args, StringBuilder b) {
    b.append(AuditConstants.PAIR_SEPARATOR).append(args.getArgs());
  }

  /**
   * Builder to create and pass a list of arbitrary key value pairs for logging.
   */
  public static class ArgsBuilder {
    private StringBuilder b;

    public ArgsBuilder() {
      b = new StringBuilder();
    }

    public ArgsBuilder append(Keys key, String value) {
      if (b.length() != 0) {
        b.append(AuditConstants.PAIR_SEPARATOR);
      }
      b.append(key.name()).append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
      return this;
    }

    public StringBuilder getArgs() {
      return b;
    }
  }
}
