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

import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.loggingAuditConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Logging auditor tests.
 * By setting the auditor to raise an exception on unaudited spans,
 * it is straightforward to determine if an operation was invoked
 * outside a span: call it, and if it does not raise an exception,
 * all is good.
 */
public class TestLoggingAuditor extends AbstractAuditingTest {

  /**
   * Logging.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TestLoggingAuditor.class);

  private LoggingAuditor auditor;

  @Before
  public void setup() throws Exception {
    super.setup();
    auditor = (LoggingAuditor) getManager().getAuditor();
  }

  /**
   * Config has logging auditing and adds SimpleAWSRequestHandler
   * too, for testing of that being added to the chain.
   * @return a config
   */
  protected Configuration createConfig() {
    return loggingAuditConfig();
  }

  @Test
  public void testToStringRobustness() throws Throwable {
    // force in the toString calls so if there are NPE problems
    // they will surface irrespective of log settings
    LOG.info(getManager().toString());
    LOG.info(auditor.toString());
  }

  /**
   * Test span activity with a span being activated/deactivated
   * and verification that calls to head() succeed in the span
   * and fail outside of it.
   */
  @Test
  public void testLoggingSpan() throws Throwable {
    long executionCount = 0;
    long failureCount = 0;

    // create a span
    AuditSpan span = span();

    // which is active
    assertActiveSpan(span);
    // so requests are allowed
    verifyAuditExecutionCount(0);
    head();
    verifyAuditExecutionCount(++executionCount);

    // now leave the span
    span.deactivate();

    // head calls are no longer allowed.
    verifyAuditFailureCount(failureCount);
    assertHeadUnaudited();
    verifyAuditFailureCount(++failureCount);
    verifyAuditExecutionCount(++executionCount);

    // spans can be reactivated and used.
    span.activate();
    head();
    verifyAuditExecutionCount(++executionCount);

    // its a no-op if the span is already active.
    span.activate();
    assertActiveSpan(span);

    // closing a span deactivates it.
    span.close();

    // IO on unaudited spans
    assertHeadUnaudited();
    verifyAuditFailureCount(++failureCount);
    verifyAuditExecutionCount(++executionCount);

    // and it is harmless to deactivate a span repeatedly.
    span.deactivate();
    span.deactivate();
  }

  /**
   * Some request types are allowed to execute outside of
   * a span.
   * Required as the transfer manager runs them in its threads.
   */
  @Test
  public void testCopyOutsideSpanAllowed() throws Throwable {
    getManager().beforeExecution(new CopyPartRequest());
    getManager().beforeExecution(new CompleteMultipartUploadRequest());
  }

  /**
   * Outside a span, the transfer state change setup works but
   * the call is unaudited.
   */
  @Test
  public void testTransferStateListenerOutsideSpan() throws Throwable {
    TransferStateChangeListener listener
        = getManager().createStateChangeListener();
    listener.transferStateChanged(null, null);
    assertHeadUnaudited();
  }

  /**
   * Outside a span, the transfer state change setup works but
   * the call is unaudited.
   */
  @Test
  public void testTransferStateListenerInSpan() throws Throwable {

    assertHeadUnaudited();
    AuditSpan span = span();

    // create the listener in the span
    TransferStateChangeListener listener
        = getManager().createStateChangeListener();
    span.deactivate();

    // head calls fail
    assertHeadUnaudited();

    // until the state change switches this thread back to the span
    listener.transferStateChanged(null, null);

    // which can be probed
    assertActiveSpan(span);

    // and executed within
    head();
  }

  /**
   * You cannot deactivate the unbonded span.
   */
  @Test
  public void testUnbondedSpanWillNotDeactivate() throws Throwable {
    AuditSpan span = activeSpan();
    // the active span is unbonded
    assertUnbondedSpan(span);
    // deactivate it.
    span.deactivate();
    // it is still the active span.
    assertActiveSpan(span);
  }

  /**
   * Spans have a different ID.
   * This is clearly not an exhaustive test.
   */
  @Test
  public void testSpanIdsAreDifferent() throws Throwable {
    AuditSpan s1 = span();
    AuditSpan s2 = span();
    assertThat(s1.getSpanId())
        .doesNotMatch(s2.getSpanId());
  }
}
