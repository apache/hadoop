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

import java.nio.file.AccessDeniedException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_FAILURE;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_REQUEST_EXECUTION;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.enableLoggingAuditor;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.resetAuditOptions;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_REQUEST_HANDLERS;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.UNAUDITED_OPERATION;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupCounterStatistic;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test audit manager invocation by making assertions
 * about the statistics of audit request execution
 * {@link org.apache.hadoop.fs.s3a.Statistic#AUDIT_REQUEST_EXECUTION}
 * and
 * {@link org.apache.hadoop.fs.s3a.Statistic#AUDIT_FAILURE}.
 */
public class ITestAuditManager extends AbstractS3ACostTest {

  public ITestAuditManager() {
    super(true);
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    resetAuditOptions(conf);
    enableLoggingAuditor(conf);
    conf.set(AUDIT_REQUEST_HANDLERS,
        SimpleAWSRequestHandler.CLASS);
    return conf;
  }

  /**
   * Get the FS IOStatistics.
   * @return the FS live IOSTats.
   */
  private IOStatistics iostats() {
    return getFileSystem().getIOStatistics();
  }

  /**
   * Verify that operations outside a span are rejected
   * by ensuring that the thread is outside a span, create
   * a write operation helper, then
   * reject it.
   */
  @Test
  public void testInvokeOutOfSpanRejected() throws Throwable {
    describe("Operations against S3 will be rejected outside of a span");
    final S3AFileSystem fs = getFileSystem();
    final long failures0 = lookupCounterStatistic(iostats(),
        AUDIT_FAILURE.getSymbol());
    final long exec0 = lookupCounterStatistic(iostats(),
        AUDIT_REQUEST_EXECUTION.getSymbol());
    // API call
    // create and close a span, so the FS is not in a span.
    fs.createSpan("span", null, null).close();

    // this will be out of span
    final WriteOperationHelper writer
        = fs.getWriteOperationHelper();

    // which can be verified
    Assertions.assertThat(writer.getAuditSpan())
        .matches(s -> !s.isValidSpan(), "Span is not valid");

    // an S3 API call will fail and be mapped to access denial.
    final AccessDeniedException ex = intercept(
        AccessDeniedException.class, UNAUDITED_OPERATION, () ->
            writer.listMultipartUploads("/"));

    // verify the type of the inner cause, throwing the outer ex
    // if it is null or a different class
    if (!(ex.getCause() instanceof AuditFailureException)) {
      throw ex;
    }

    assertThatStatisticCounter(iostats(), AUDIT_REQUEST_EXECUTION.getSymbol())
        .isGreaterThan(exec0);
    assertThatStatisticCounter(iostats(), AUDIT_FAILURE.getSymbol())
        .isGreaterThan(failures0);
  }

  @Test
  public void testRequestHandlerBinding() throws Throwable {
    describe("Verify that extra request handlers can be added and that they"
        + " will be invoked during request execution");
    final long baseCount = SimpleAWSRequestHandler.getInvocationCount();
    final S3AFileSystem fs = getFileSystem();
    final long exec0 = lookupCounterStatistic(iostats(),
        AUDIT_REQUEST_EXECUTION.getSymbol());
    // API call to a known path, `getBucketLocation()` does not always result in an API call.
    fs.listStatus(path("/"));
    // which MUST have ended up calling the extension request handler
    Assertions.assertThat(SimpleAWSRequestHandler.getInvocationCount())
        .describedAs("Invocation count of plugged in request handler")
        .isGreaterThan(baseCount);
    assertThatStatisticCounter(iostats(), AUDIT_REQUEST_EXECUTION.getSymbol())
        .isGreaterThan(exec0);
    assertThatStatisticCounter(iostats(), AUDIT_FAILURE.getSymbol())
        .isZero();
  }
}
