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

package org.apache.hadoop.fs.audit;

/**
 * Statistic Names for Auditing.
 */
public final class AuditStatisticNames {

  private AuditStatisticNames() {
  }

  /**
   * Audit failure: {@value}.
   */
  public static final String AUDIT_FAILURE = "audit_failure";

  /**
   * A request was executed and the auditor invoked: {@value}.
   */
  public static final String AUDIT_REQUEST_EXECUTION
      = "audit_request_execution";

  /**
   * Audit span created: {@value}.
   */
  public static final String AUDIT_SPAN_CREATION = "audit_span_creation";

  /**
   * Access check during audit rejected: {@value}.
   */
  public static final String AUDIT_ACCESS_CHECK_FAILURE
      = "audit_access_check_failure";
}
