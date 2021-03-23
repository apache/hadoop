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
import org.apache.hadoop.fs.s3a.audit.AuditSpan;
import org.apache.hadoop.fs.s3a.audit.OperationAuditor;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * An audit service which returns the {@link NoopSpan}.
 */
public final class NoopAuditor extends AbstractOperationAuditor {

  /**
  * unbonded span created in constructor.
  */
  private final AuditSpan unbondedSpan;

  private final NoopSpan.SpanActivationCallbacks
      activationCallbacks;

  /**
   * Constructor.
   * @param name name
   * @param activationCallbacks Activation callbacks.
   */
  public NoopAuditor(final String name,
      NoopSpan.SpanActivationCallbacks activationCallbacks) {
    super(name, iostatisticsStore().build());
    unbondedSpan = createSpan("unbonded", null, null);
    this.activationCallbacks = activationCallbacks;
  }

  @Override
  public AuditSpan createSpan(final String name,
      @Nullable final String path1,
      @Nullable final String path2)  {
    return new NoopSpan(createSpanID(), name, path1, path2,
        activationCallbacks);
  }

  @Override
  public AuditSpan getUnbondedSpan() {
    return unbondedSpan;
  }

  /**
   * Create, init and start an instance.
   * @param conf configuration.
   * @param activationCallbacks Activation callbacks.
   * @return a started instance.
   */
  public static NoopAuditor newInstance(Configuration conf,
      NoopSpan.SpanActivationCallbacks activationCallbacks) {
    NoopAuditor noop = new NoopAuditor("noop",
        activationCallbacks);
    noop.init(conf);
    noop.start();
    return noop;
  }

}
