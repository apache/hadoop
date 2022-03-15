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

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.s3a.audit.OperationAuditorOptions;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * An audit service which returns the {@link NoopSpan}.
 * Even though the spans are no-ops, each span is still
 * created with a unique span ID.
 */
public class NoopAuditor extends AbstractOperationAuditor {

  /**
   * unbonded span created in constructor.
   */
  private final AuditSpanS3A unbondedSpan;

  /**
   * Activation callbacks.
   */
  private final NoopSpan.SpanActivationCallbacks activationCallbacks;

  /**
   * Constructor.
   * This will be used when the auditor is created through
   * configuration and classloading.
   */
  public NoopAuditor() {
    this(null);
  }

  /**
   * Constructor when explicitly created within
   * the {@link NoopAuditManagerS3A}.
   * @param activationCallbacks Activation callbacks.
   */
  public NoopAuditor(
      NoopSpan.SpanActivationCallbacks activationCallbacks) {
    super("NoopAuditor");
    this.unbondedSpan = createSpan("unbonded", null, null);
    this.activationCallbacks = activationCallbacks;
  }

  @Override
  public AuditSpanS3A createSpan(
      final String operation,
      @Nullable final String path1,
      @Nullable final String path2) {
    return new NoopSpan(createSpanID(), operation, path1, path2,
        activationCallbacks);
  }

  @Override
  public AuditSpanS3A getUnbondedSpan() {
    return unbondedSpan;
  }

  /**
   * Create, init and start an instance.
   * @param conf configuration.
   * @param activationCallbacks Activation callbacks.
   * @return a started instance.
   */
  public static NoopAuditor createAndStartNoopAuditor(Configuration conf,
      NoopSpan.SpanActivationCallbacks activationCallbacks) {
    NoopAuditor noop = new NoopAuditor(activationCallbacks);
    final OperationAuditorOptions options =
        OperationAuditorOptions.builder()
            .withConfiguration(conf)
            .withIoStatisticsStore(iostatisticsStore().build());
    noop.init(options);
    noop.start();
    return noop;
  }

}
