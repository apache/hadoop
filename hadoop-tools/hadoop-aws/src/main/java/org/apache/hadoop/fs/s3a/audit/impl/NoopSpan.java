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

/**
 * A minimal span with no direct side effects.
 * It does have an ID and, if given callbacks,
 * will notify the callback implementation
 * of activation and deactivation.
 * Subclassable for tests.
 */
public class NoopSpan extends AbstractAuditSpanImpl {

  private final String path1;

  private final String path2;

  /** Activation callbacks. */
  private final SpanActivationCallbacks activationCallbacks;

  /**
   * Static public instance.
   */
  public static final NoopSpan INSTANCE = new NoopSpan();

  /**
   * Create a no-op span.
   * @param spanId span ID
   * @param operationName operation name
   * @param path1 path
   * @param path2 path 2
   * @param activationCallbacks Activation callbacks.
   */
  protected NoopSpan(String spanId,
      final String operationName,
      final String path1,
      final String path2,
      final SpanActivationCallbacks activationCallbacks) {
    super(spanId, operationName);
    this.path1 = path1;
    this.path2 = path2;
    this.activationCallbacks = activationCallbacks;
  }

  protected NoopSpan() {
    this("", "no-op", null, null, null);
  }


  @Override
  public AuditSpanS3A activate() {
    if (activationCallbacks != null) {
      activationCallbacks.activate(this);
    }
    return this;
  }

  @Override
  public void deactivate() {
    if (activationCallbacks != null) {
      activationCallbacks.deactivate(this);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("NoopSpan{");
    sb.append("id='").append(getSpanId()).append('\'');
    sb.append("name='").append(getOperationName()).append('\'');
    sb.append(", path1='").append(path1).append('\'');
    sb.append(", path2='").append(path2).append('\'');
    sb.append('}');
    return sb.toString();
  }

  /** Activation callbacks. */
  public interface SpanActivationCallbacks {

    /**
     * Span was activated.
     * @param span span reference.
     */
    void activate(AuditSpanS3A span);

    /**
     * Span was deactivated.
     * @param span span reference.
     */
    void deactivate(AuditSpanS3A span);
  }
}
