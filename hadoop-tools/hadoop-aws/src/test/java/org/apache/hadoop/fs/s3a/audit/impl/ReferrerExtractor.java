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

import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.store.audit.HttpReferrerAuditHeader;

/**
 * Extract the referrer from a LoggingAuditor through a package-private
 * method.
 */
public final class ReferrerExtractor {

  private ReferrerExtractor() {
  }

  /**
   * Get the referrer provided the span is an instance or
   * subclass of LoggingAuditSpan.
   * If wrapped by a {@code WrappingAuditSpan}, it will be extracted.
   * @param auditor the auditor.
   * @param span span
   * @return the referrer
   * @throws ClassCastException if a different span type was passed in
   */
  public static HttpReferrerAuditHeader getReferrer(LoggingAuditor auditor,
      AuditSpanS3A span) {
    AuditSpanS3A sp;
    if (span instanceof ActiveAuditManagerS3A.WrappingAuditSpan) {
      sp = ((ActiveAuditManagerS3A.WrappingAuditSpan) span).getSpan();
    } else {
      sp = span;
    }
    return auditor.getReferrer(sp);
  }
}
