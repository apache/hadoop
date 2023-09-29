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
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.util.Time;

import static java.util.Objects.requireNonNull;

/**
 * Base class for the audit spans implementations..
 */
public abstract class AbstractAuditSpanImpl implements AuditSpanS3A {

  /**
   * Span ID.
   */
  private final String spanId;

  /**
   * Timestamp in UTC of span creation.
   */
  private final long timestamp;

  private final String operationName;

  /**
   * Constructor.
   * @param spanId span ID.
   * @param operationName operation name
   */
  protected AbstractAuditSpanImpl(
      final String spanId,
      final String operationName) {
    this(spanId, Time.now(), operationName);
  }

  /**
   * Constructor.
   * @param spanId span ID.
   * @param timestamp timestamp in millis
   * @param operationName operation name
   */
  protected AbstractAuditSpanImpl(
      final String spanId,
      final long timestamp,
      final String operationName) {
    this.spanId = requireNonNull(spanId);
    this.timestamp = timestamp;
    this.operationName = operationName;
  }

  @Override
  public final String getSpanId() {
    return spanId;
  }

  @Override
  public String getOperationName() {
    return operationName;
  }

  @Override
  public final long getTimestamp() {
    return timestamp;
  }

  @Override
  public AuditSpanS3A activate() {
    return this;
  }

  /**
   * Invoke {@link AuditSpan#deactivate()}.
   * This is final: subclasses MUST override the
   * {@code deactivate()} method.
   */
  @Override
  public final void close() {
    deactivate();
  }
}
