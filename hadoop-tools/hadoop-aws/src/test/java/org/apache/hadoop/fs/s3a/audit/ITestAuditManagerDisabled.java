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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditManagerS3A;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.NOOP_SPAN;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.resetAuditOptions;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_ENABLED;

/**
 * Verify that audit managers are disabled if set to false.
 */
public class ITestAuditManagerDisabled extends AbstractS3ACostTest {

  public ITestAuditManagerDisabled() {
    super(true);
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    resetAuditOptions(conf);
    conf.setBoolean(AUDIT_ENABLED, false);
    return conf;
  }

  /**
   * Verify that the auditor is the no-op auditor if auditing is disabled.
   */
  @Test
  public void testAuditorDisabled() {

    final S3AFileSystem fs = getFileSystem();
    final AuditManagerS3A auditManager = fs.getAuditManager();

    Assertions.assertThat(auditManager)
        .isInstanceOf(NoopAuditManagerS3A.class);
  }

  /**
   * All the audit spans are the no-op span.
   */
  @Test
  public void testAuditSpansAreAllTheSame() throws Throwable {

    final S3AFileSystem fs = getFileSystem();
    final AuditSpanS3A span1 = fs.createSpan("span1", null, null);
    final AuditSpanS3A span2 = fs.createSpan("span2", null, null);
    Assertions.assertThat(span1)
        .describedAs("audit span 1")
        .isSameAs(NOOP_SPAN);
    Assertions.assertThat(span2)
        .describedAs("audit span 2")
        .isSameAs(span1);
  }
}
