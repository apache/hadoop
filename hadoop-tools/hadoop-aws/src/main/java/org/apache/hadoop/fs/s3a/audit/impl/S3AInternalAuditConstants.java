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

package org.apache.hadoop.fs.s3a.audit.impl;

import software.amazon.awssdk.core.interceptor.ExecutionAttribute;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;

/**
 * Internal constants; not intended for public use, or
 * for use by any external implementations.
 */
@InterfaceAudience.Private
public final class S3AInternalAuditConstants {

  private S3AInternalAuditConstants() {
  }

  /**
   * Exceution attribute for audit span callbacks.
   * This is used to retrieve the span in the AWS code.
   */
  public static final ExecutionAttribute<AuditSpanS3A>
      AUDIT_SPAN_EXECUTION_ATTRIBUTE =
      new ExecutionAttribute<>(
          "org.apache.hadoop.fs.s3a.audit.AuditSpanS3A");
}
