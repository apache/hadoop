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

import java.util.List;

import com.amazonaws.handlers.RequestHandler2;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.AuditConstants;
import org.apache.hadoop.fs.s3a.audit.AuditIntegration;
import org.apache.hadoop.fs.s3a.audit.AuditSpan;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * Unit tests related to audit manager and span lifecycle.
 */
public class TestAuditManagerLifecycle extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAuditManagerLifecycle.class);

  private static final IOStatisticsStore STORE
      = IOStatisticsBinding.emptyStatisticsStore();

  private Configuration conf;

  private ActiveAuditManager auditManager;

  private AuditSpan resetSpan;

  @Before
  public void setup() throws Exception {
    // conf is always bonded to no-op service
    conf = new Configuration(false);
    conf.set(AuditConstants.AUDIT_SERVICE_CLASSNAME,
        AuditConstants.NOOP_AUDIT_SERVICE);
    auditManager = (ActiveAuditManager)
        AuditIntegration.createAuditManager(conf, STORE);
    resetSpan = auditManager.getActiveThreadSpan();

  }

  @After
  public void teardown() throws Exception {
    closeQuietly(auditManager);
  }

  /**
   * Core lifecycle (remember: the service has already been started).
   */
  @Test
  public void testStop() throws Throwable {
    auditManager.stop();
  }

  @Test
  public void testCreateRequestHandlers() throws Throwable {
    List<RequestHandler2> handlers
        = auditManager.createRequestHandlers();
    Assertions.assertThat(handlers)
        .isNotEmpty();
  }

  @Test
  public void testInitialSpanIsInvalid() throws Throwable {
    Assertions.assertThat(resetSpan)
        .matches(f -> !f.isValidSpan(), "is invalid");
  }

  @Test
  public void testCreateCloseSpan() throws Throwable {
    AuditSpan span = auditManager.createSpan("op", null, null);
    Assertions.assertThat(span)
        .matches(AuditSpan::isValidSpan, "is valid");
    assertActiveSpan(span);
    // activation when already active is no-op
    span.activate();
    assertActiveSpan(span);
    // close the span
    span.close();
    // the original span is restored.
    assertActiveSpan(resetSpan);
  }

  @Test
  @Ignore
  public void testSpanActivation() throws Throwable {
    // real activation switches spans in the current thead.

    AuditSpan span1 = auditManager.createSpan("op1", null, null);
    AuditSpan span2 = auditManager.createSpan("op2", null, null);
    assertActiveSpan(span2);
    // switch back to span 1
    span1.activate();
    assertActiveSpan(span1);
    // then to span 2
    span2.activate();
    assertActiveSpan(span2);
    span2.close();
    // because span2 was active at time of close,
    // we revert to whatever span was active when it was started.
    assertActiveSpan(span1);
  }

  @Test
  @Ignore
  public void testSpanDeactivation() throws Throwable {
    AuditSpan span1 = auditManager.createSpan("op1", null, null);
    AuditSpan span2 = auditManager.createSpan("op2", null, null);
    assertActiveSpan(span2);

    span1.close();
    // because span2 was active at time of close,
    // we revert to whatever span was active when it was started.
    assertActiveSpan(span2);
    // when span2 is closed, it will go back to the reset
    // span because span1 is now closed
    // that is closed
    span2.close();
    assertActiveSpan(resetSpan);
  }

  @Test
  @Ignore
  public void testResetSpanCannotBeClosed() throws Throwable {

    Assertions.assertThat(resetSpan)
        .matches(f -> !f.isValidSpan(), "is invalid");
    // create a new span
    AuditSpan span1 = auditManager.createSpan("op1", null, null);
    // switch to the reset span and then close it.
    resetSpan.activate();
    resetSpan.close();
    assertActiveSpan(resetSpan);
    span1.close();
  }

  /**
   * Ass that the supplied parameter is the active span
   * for this thread.
   * @param span span expected to be active
   */
  private void assertActiveSpan(AuditSpan span) {
    Assertions.assertThat(auditManager.getActiveThreadSpan())
        .isSameAs(span);
  }

}
