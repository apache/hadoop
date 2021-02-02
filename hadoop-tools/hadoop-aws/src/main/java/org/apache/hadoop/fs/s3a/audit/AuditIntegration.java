/*
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

package org.apache.hadoop.fs.s3a.audit;

import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.InvocationRaisingIOE;

/**
 * Support for integrating auditing within the S3A code.
 */
public final class AuditIntegration {

  private AuditIntegration() {
  }

  /**
   * Given a callable, return a new callable which
   * activates and deactivates the span around the inner invocation.
   * @param auditSpan audit span
   * @param operation operation
   * @param <T> type of result
   * @return a new invocation.
   */
  public static <T> CallableRaisingIOE<T> withinSpan(
      AuditSpan auditSpan,
      CallableRaisingIOE<T> operation) {
    return () -> {
      auditSpan.activate();
      try {
        return operation.apply();
      } finally {
        auditSpan.deactivate();
      }
    };
  }

  /**
   * Given a invocation, return a new invocation which
   * activates and deactivates the span around the inner invocation.
   * @param auditSpan audit span
   * @param operation operation
   * @return a new invocation.
   */
  public static InvocationRaisingIOE withinSpan(
      AuditSpan auditSpan,
      InvocationRaisingIOE operation) {
    return () -> {
      auditSpan.activate();
      try {
        operation.apply();;
      } finally {
        auditSpan.deactivate();
      }
    };
  }

}
