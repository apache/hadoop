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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditManagerS3A;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditor;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_ACCESS_CHECK_FAILURE;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_FAILURE;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_REQUEST_EXECUTION;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_SPAN_CREATION;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_ENABLED;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_EXECUTION_INTERCEPTORS;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_SERVICE_CLASSNAME;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.LOGGING_AUDIT_SERVICE;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.NOOP_AUDIT_SERVICE;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REFERRER_HEADER_ENABLED;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REJECT_OUT_OF_SPAN_OPERATIONS;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * Support for auditing in testing.
 */
public final class AuditTestSupport {

  private AuditTestSupport() {
  }

  /**
   * Reusable no-op span instance.
   */
  public static final AuditSpan NOOP_SPAN = NoopAuditManagerS3A
      .createNewSpan("noop", null, null);

  /**
   * Create, init and start a no-op auditor instance.
   * @param conf configuration.
   * @return a started instance.
   */
  public static OperationAuditor noopAuditor(Configuration conf) {
    return NoopAuditor.createAndStartNoopAuditor(conf, null);
  }

  /**
   * Create config for no-op auditor.
   * @return config with nothing but the no-op audit service set up.
   */
  public static Configuration noopAuditConfig() {
    final Configuration conf = new Configuration(false);
    conf.set(
        AUDIT_SERVICE_CLASSNAME, NOOP_AUDIT_SERVICE);
    conf.setBoolean(AUDIT_ENABLED, true);
    return conf;
  }

  /**
   * Create config for logging auditor which
   * rejects out of span operations.
   * @return config
   */
  public static Configuration loggingAuditConfig() {
    return enableLoggingAuditor(new Configuration(false));
  }

  /**
   * Patch the configuration to support the logging auditor and
   * rejects out of span operations.
   * @param conf config to patch.
   * @return the config
   */
  public static Configuration enableLoggingAuditor(final Configuration conf) {
    conf.set(AUDIT_SERVICE_CLASSNAME, LOGGING_AUDIT_SERVICE);
    conf.setBoolean(AUDIT_ENABLED, true);
    conf.setBoolean(REJECT_OUT_OF_SPAN_OPERATIONS,  true);
    return conf;
  }

  /**
   * Create IOStatistics store with the auditor counters wired up.
   * @return an IOStatistics store to pass to audit managers.
   */
  public static IOStatisticsStore createIOStatisticsStoreForAuditing() {
    return iostatisticsStore()
        .withCounters(
            AUDIT_ACCESS_CHECK_FAILURE.getSymbol(),
            AUDIT_FAILURE.getSymbol(),
            AUDIT_REQUEST_EXECUTION.getSymbol(),
            AUDIT_SPAN_CREATION.getSymbol())
        .build();
  }

  /**
   * Remove all overridden values for
   * the test bucket/global in the given config.
   * @param conf configuration to patch
   * @return the configuration.
   */
  public static Configuration resetAuditOptions(Configuration conf) {
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
        REFERRER_HEADER_ENABLED,
        REJECT_OUT_OF_SPAN_OPERATIONS,
        AUDIT_EXECUTION_INTERCEPTORS,
        AUDIT_SERVICE_CLASSNAME,
        AUDIT_ENABLED);
    return conf;
  }
}
