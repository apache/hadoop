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
package org.apache.hadoop.security;

import static org.apache.hadoop.security.UGIExceptionMessages.*;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Thrown when {@link UserGroupInformation} failed with an unrecoverable error,
 * such as failure in kerberos login/logout, invalid subject etc.
 *
 * Caller should not retry when catching this exception.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class KerberosAuthException extends IOException {
  static final long serialVersionUID = 31L;

  private String user;
  private String principal;
  private String keytabFile;
  private String ticketCacheFile;
  private String initialMessage;

  public KerberosAuthException(String msg) {
    super(msg);
  }

  public KerberosAuthException(Throwable cause) {
    super(cause);
  }

  public KerberosAuthException(String initialMsg, Throwable cause) {
    this(cause);
    initialMessage = initialMsg;
  }

  public void setUser(final String u) {
    user = u;
  }

  public void setPrincipal(final String p) {
    principal = p;
  }

  public void setKeytabFile(final String k) {
    keytabFile = k;
  }

  public void setTicketCacheFile(final String t) {
    ticketCacheFile = t;
  }

  /** @return The initial message, or null if not set. */
  public String getInitialMessage() {
    return initialMessage;
  }

  /** @return The keytab file path, or null if not set. */
  public String getKeytabFile() {
    return keytabFile;
  }

  /** @return The principal, or null if not set. */
  public String getPrincipal() {
    return principal;
  }

  /** @return The ticket cache file path, or null if not set. */
  public String getTicketCacheFile() {
    return ticketCacheFile;
  }

  /** @return The user, or null if not set. */
  public String getUser() {
    return user;
  }

  @Override
  public String getMessage() {
    final StringBuilder sb = new StringBuilder();
    if (initialMessage != null) {
      sb.append(initialMessage);
    }
    if (user != null) {
      sb.append(FOR_USER + user);
    }
    if (principal != null) {
      sb.append(FOR_PRINCIPAL + principal);
    }
    if (keytabFile != null) {
      sb.append(FROM_KEYTAB + keytabFile);
    }
    if (ticketCacheFile != null) {
      sb.append(USING_TICKET_CACHE_FILE+ ticketCacheFile);
    }
    sb.append(" " + super.getMessage());
    return sb.toString();
  }
}