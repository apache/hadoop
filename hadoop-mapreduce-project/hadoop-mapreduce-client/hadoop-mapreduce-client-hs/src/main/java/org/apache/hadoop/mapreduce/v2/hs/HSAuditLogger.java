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

package org.apache.hadoop.mapreduce.v2.hs;

import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.Server;

@Private
public class HSAuditLogger {
  private static final Log LOG = LogFactory.getLog(HSAuditLogger.class);

  static enum Keys {
    USER, OPERATION, TARGET, RESULT, IP, PERMISSIONS, DESCRIPTION
  }

  public static class AuditConstants {
    static final String SUCCESS = "SUCCESS";
    static final String FAILURE = "FAILURE";
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';

    // Some commonly used descriptions
    public static final String UNAUTHORIZED_USER = "Unauthorized user";
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   * 
   * @param user
   *          User who made the service request.
   * @param operation
   *          Operation requested by the user.
   * @param target
   *          The target on which the operation is being performed.
   * 
   * <br>
   * <br>
   *          Note that the {@link HSAuditLogger} uses tabs ('\t') as a key-val
   *          delimiter and hence the value fields should not contains tabs
   *          ('\t').
   */
  public static void logSuccess(String user, String operation, String target) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target));
    }
  }

  /**
   * A helper api for creating an audit log for a successful event.
   */
  static String createSuccessLog(String user, String operation, String target) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target, b);
    add(Keys.RESULT, AuditConstants.SUCCESS, b);
    return b.toString();
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
   * Appends the key-val pair to the passed builder in the following format
   * <pair-delim>key=value
   */
  static void add(Keys key, String value, StringBuilder b) {
    b.append(AuditConstants.PAIR_SEPARATOR).append(key.name())
        .append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * Adds the first key-val pair to the passed builder in the following format
   * key=value
   */
  static void start(Keys key, String value, StringBuilder b) {
    b.append(key.name()).append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   * 
   * @param user
   *          User who made the service request.
   * @param operation
   *          Operation requested by the user.
   * @param perm
   *          Target permissions.
   * @param target
   *          The target on which the operation is being performed.
   * @param description
   *          Some additional information as to why the operation failed.
   * 
   * <br>
   * <br>
   *          Note that the {@link HSAuditLogger} uses tabs ('\t') as a key-val
   *          delimiter and hence the value fields should not contains tabs
   *          ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description));
    }
  }

  /**
   * A helper api for creating an audit log for a failure event.
   */
  static String createFailureLog(String user, String operation, String perm,
      String target, String description) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target, b);
    add(Keys.RESULT, AuditConstants.FAILURE, b);
    add(Keys.DESCRIPTION, description, b);
    add(Keys.PERMISSIONS, perm, b);

    return b.toString();
  }
}
