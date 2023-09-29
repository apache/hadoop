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
package org.apache.hadoop.crypto.key.kms.server;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMSACLs.Type;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Interface defining a KMS audit logger.
 * <p>
 * IMPORTANT WARNING: Audit logs should be strictly backwards-compatible,
 * because there are usually parsing tools highly dependent on the audit log
 * formatting. Different tools have different ways of parsing the audit log, so
 * changing the audit log output in any way is considered incompatible,
 * and will haunt the consumer tools / developers. Don't do it.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
interface KMSAuditLogger {
  enum OpStatus {
    OK, UNAUTHORIZED, UNAUTHENTICATED, ERROR;
  }

  /**
   * Class defining an audit event.
   */
  class AuditEvent {
    private final AtomicLong accessCount = new AtomicLong(-1);
    private final Object op;
    private final String keyName;
    private final String user;
    private final String impersonator;
    private final String remoteHost;
    private final String extraMsg;
    private final long startTime = System.currentTimeMillis();
    private long endTime = startTime;

    /**
     * @param op
     *          The operation being audited (either {@link KMS.KMSOp} or
     *          {@link Type} N.B this is passed as an {@link Object} to allow
     *          either enum to be passed in.
     * @param ugi
     *          The user's security context
     * @param keyName
     *          The String name of the key if applicable
     * @param remoteHost
     *          The hostname of the requesting service
     * @param msg
     *          Any extra details for auditing
     */
    AuditEvent(Object op, UserGroupInformation ugi, String keyName,
        String remoteHost, String msg) {
      this.keyName = keyName;
      if (ugi == null) {
        this.user = null;
        this.impersonator = null;
      } else {
        this.user = ugi.getUserName();
        if (ugi.getAuthenticationMethod()
            == UserGroupInformation.AuthenticationMethod.PROXY) {
          this.impersonator = ugi.getRealUser().getUserName();
        } else {
          this.impersonator = null;
        }
      }
      this.remoteHost = remoteHost;
      this.op = op;
      this.extraMsg = msg;
    }

    public AtomicLong getAccessCount() {
      return accessCount;
    }

    public Object getOp() {
      return op;
    }

    public String getKeyName() {
      return keyName;
    }

    public String getUser() {
      return user;
    }

    public String getImpersonator() {
      return impersonator;
    }

    public String getRemoteHost() {
      return remoteHost;
    }

    public String getExtraMsg() {
      return extraMsg;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }

    /**
     * Set the time this audit event is finished.
     */
    void setEndTime(long endTime) {
      this.endTime = endTime;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("op=" + op).append(", keyName=" + keyName)
          .append(", user=" + user).append(", impersonator=" + impersonator)
          .append(", remoteHost=" + remoteHost)
          .append(", extraMsg=" + extraMsg);
      return sb.toString();
    }
  }

  /**
   * Clean up the audit logger.
   *
   * @throws IOException
   */
  void cleanup() throws IOException;

  /**
   * Initialize the audit logger.
   *
   * @param conf The configuration object.
   * @throws IOException
   */
  void initialize(Configuration conf) throws IOException;

  /**
   * Log an audit event.
   *
   * @param status The status of the event.
   * @param event  The audit event.
   */
  void logAuditEvent(final OpStatus status, final AuditEvent event);
}