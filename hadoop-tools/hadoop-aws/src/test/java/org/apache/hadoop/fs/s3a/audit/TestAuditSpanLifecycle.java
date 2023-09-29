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

import java.util.List;

import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.store.audit.AuditSpan;


import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.noopAuditConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests related to span lifecycle.
 */
public class TestAuditSpanLifecycle extends AbstractAuditingTest {

  private AuditSpan resetSpan;

  @Before
  public void setup() throws Exception {
    super.setup();
    resetSpan = getManager().getActiveAuditSpan();
  }

  protected Configuration createConfig() {
    return noopAuditConfig();
  }

  /**
   * Core lifecycle (remember: the service has already been started).
   */
  @Test
  public void testStop() throws Throwable {
    getManager().stop();
  }

  @Test
  public void testCreateExecutionInterceptors() throws Throwable {
    List<ExecutionInterceptor> interceptors
        = getManager().createExecutionInterceptors();
    assertThat(interceptors).isNotEmpty();
  }

  @Test
  public void testInitialSpanIsInvalid() throws Throwable {
    assertThat(resetSpan)
        .matches(f -> !f.isValidSpan(), "is invalid");
  }

  @Test
  public void testCreateCloseSpan() throws Throwable {
    AuditSpan span = getManager().createSpan("op", null, null);
    assertThat(span)
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
  public void testSpanActivation() throws Throwable {
    // real activation switches spans in the current thead.

    AuditSpan span1 = getManager().createSpan("op1", null, null);
    AuditSpan span2 = getManager().createSpan("op2", null, null);
    assertActiveSpan(span2);
    // switch back to span 1
    span1.activate();
    assertActiveSpan(span1);
    // then to span 2
    span2.activate();
    assertActiveSpan(span2);
    span2.close();

    assertActiveSpan(resetSpan);
    span1.close();
    assertActiveSpan(resetSpan);
  }

  @Test
  public void testSpanDeactivation() throws Throwable {
    AuditSpan span1 = getManager().createSpan("op1", null, null);
    AuditSpan span2 = getManager().createSpan("op2", null, null);
    assertActiveSpan(span2);

    // this doesn't close as it is not active
    span1.close();
    assertActiveSpan(span2);
    span2.close();
    assertActiveSpan(resetSpan);
  }

  @Test
  public void testResetSpanCannotBeClosed() throws Throwable {

    assertThat(resetSpan)
        .matches(f -> !f.isValidSpan(), "is invalid");
    // create a new span
    AuditSpan span1 = getManager().createSpan("op1", null, null);
    // switch to the reset span and then close it.
    resetSpan.activate();
    resetSpan.close();
    assertActiveSpan(resetSpan);
    span1.close();
  }

}
