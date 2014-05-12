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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;

/**
 * Provides convenience methods for audit logging consistently the different
 * types of events.
 */
public class KMSAudit {
  public static final String KMS_LOGGER_NAME = "kms-audit";

  private static Logger AUDIT_LOG = LoggerFactory.getLogger(KMS_LOGGER_NAME);

  private static void op(String status, String op, Principal user, String key,
      String extraMsg) {
    AUDIT_LOG.info("Status:{} User:{} Op:{} Name:{}{}", status, user.getName(),
        op, key, extraMsg);
  }

  public static void ok(Principal user, String op, String key,
      String extraMsg) {
    op("OK", op, user, key, extraMsg);
  }

  public static void unauthorized(Principal user, String op, String key) {
    op("UNAUTHORIZED", op, user, key, "");
  }

  public static void error(Principal user, String method, String url,
      String extraMsg) {
    AUDIT_LOG.info("Status:ERROR User:{} Method:{} URL:{} Exception:'{}'",
        user.getName(), method, url, extraMsg);
  }

  public static void unauthenticated(String remoteHost, String method,
      String url, String extraMsg) {
    AUDIT_LOG.info(
        "Status:UNAUTHENTICATED RemoteHost:{} Method:{} URL:{} ErrorMsg:'{}'",
        remoteHost, method, url, extraMsg);
  }

}
