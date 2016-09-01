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

import static org.apache.hadoop.crypto.key.kms.server.KMSAudit.KMS_LOGGER_NAME;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple text format audit logger. This is the default.
 * <p>
 * IMPORTANT WARNING: Audit logs should be strictly backwards-compatible,
 * because there are usually parsing tools highly dependent on the audit log
 * formatting. Different tools have different ways of parsing the audit log, so
 * changing the audit log output in any way is considered incompatible,
 * and will haunt the consumer tools / developers. Don't do it.
 */
class SimpleKMSAuditLogger implements KMSAuditLogger {
  final private Logger auditLog = LoggerFactory.getLogger(KMS_LOGGER_NAME);

  @Override
  public void cleanup() throws IOException {
  }

  @Override
  public void initialize(Configuration conf) throws IOException {
  }

  @Override
  public void logAuditEvent(final OpStatus status, final AuditEvent event) {
    if (!Strings.isNullOrEmpty(event.getUser()) && !Strings
        .isNullOrEmpty(event.getKeyName()) && (event.getOp() != null)
        && KMSAudit.AGGREGATE_OPS_WHITELIST.contains(event.getOp())) {
      switch (status) {
      case OK:
        auditLog.info(
            "{}[op={}, key={}, user={}, accessCount={}, interval={}ms] {}",
            status, event.getOp(), event.getKeyName(), event.getUser(),
            event.getAccessCount().get(),
            (event.getEndTime() - event.getStartTime()), event.getExtraMsg());
        break;
      case UNAUTHORIZED:
        logAuditSimpleFormat(status, event);
        break;
      default:
        logAuditSimpleFormat(status, event);
        break;
      }
    } else {
      logAuditSimpleFormat(status, event);
    }
  }

  private void logAuditSimpleFormat(final OpStatus status,
      final AuditEvent event) {
    final List<String> kvs = new LinkedList<>();
    if (event.getOp() != null) {
      kvs.add("op=" + event.getOp());
    }
    if (!Strings.isNullOrEmpty(event.getKeyName())) {
      kvs.add("key=" + event.getKeyName());
    }
    if (!Strings.isNullOrEmpty(event.getUser())) {
      kvs.add("user=" + event.getUser());
    }
    if (kvs.isEmpty()) {
      auditLog.info("{} {}", status, event.getExtraMsg());
    } else {
      final String join = Joiner.on(", ").join(kvs);
      auditLog.info("{}[{}] {}", status, join, event.getExtraMsg());
    }
  }
}
