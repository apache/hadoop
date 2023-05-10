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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.RequestFactoryImpl;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_GET_FILE_STATUS;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.UNAUDITED_OPERATION;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.createIOStatisticsStoreForAuditing;
import static org.apache.hadoop.service.ServiceOperations.stopQuietly;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract class for auditor unit tests.
 */
public abstract class AbstractAuditingTest extends AbstractHadoopTestBase {

  protected static final String OPERATION
      = INVOCATION_GET_FILE_STATUS.getSymbol();

  /**
   * Logging.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractAuditingTest.class);

  public static final String PATH_1 = "/path1";

  public static final String PATH_2 = "/path2";

  /**
   * Statistics store with the auditor counters wired up.
   */
  private final IOStatisticsStore ioStatistics =
      createIOStatisticsStoreForAuditing();

  private RequestFactory requestFactory;

  private AuditManagerS3A manager;

  @Before
  public void setup() throws Exception {
    requestFactory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .build();
    manager = AuditIntegration.createAndStartAuditManager(
        createConfig(),
        ioStatistics);
  }

  /**
   * Create config.
   * @return config to use when creating a manager
   */
  protected abstract Configuration createConfig();

  @After
  public void teardown() {
    stopQuietly(manager);
  }

  protected IOStatisticsStore getIOStatistics() {
    return ioStatistics;
  }

  protected RequestFactory getRequestFactory() {
    return requestFactory;
  }

  protected AuditManagerS3A getManager() {
    return manager;
  }

  /**
   * Assert that a specific span is active.
   * This matches on the wrapped spans.
   * @param span span to assert over.
   */
  protected void assertActiveSpan(final AuditSpan span) {
    assertThat(activeSpan())
        .isSameAs(span);
  }

  /**
   * Assert a span is unbound/invalid.
   * @param span span to assert over.
   */
  protected void assertUnbondedSpan(final AuditSpan span) {
    assertThat(span.isValidSpan())
        .describedAs("Validity of %s", span)
        .isFalse();
  }

  protected AuditSpanS3A activeSpan() {
    return manager.getActiveAuditSpan();
  }

  /**
   * Create a head request and pass it through the manager's beforeExecution()
   * callback.
   * @return a processed request.
   */
  protected GetObjectMetadataRequest head() {
    return manager.beforeExecution(
        requestFactory.newGetObjectMetadataRequest("/"));
  }

  /**
   * Create a GetObject request and modify it before passing it through auditor.
   * @param modifyRequest Consumer Interface for changing the request before passing to the auditor
   * @return the request
   */
  protected GetObjectRequest get(Consumer<GetObjectRequest> modifyRequest) {
    GetObjectRequest req = requestFactory.newGetObjectRequest("/");
    modifyRequest.accept(req);
    return manager.beforeExecution(req);
  }

  /**
   * Assert a head request fails as there is no
   * active span.
   */
  protected void assertHeadUnaudited() throws Exception {
    intercept(AuditFailureException.class,
        UNAUDITED_OPERATION, this::head);
  }

  /**
   * Assert that the audit failure is of a given value.
   * Returns the value to assist in chaining,
   * @param expected expected value
   * @return the expected value.
   */
  protected long verifyAuditFailureCount(
      final long expected) {
    return verifyCounter(Statistic.AUDIT_FAILURE, expected);
  }

  /**
   * Assert that the audit execution count
   * is of a given value.
   * Returns the value to assist in chaining,
   * @param expected expected value
   * @return the expected value.
   */
  protected long verifyAuditExecutionCount(
      final long expected) {
    return verifyCounter(Statistic.AUDIT_REQUEST_EXECUTION, expected);
  }

  /**
   * Assert that a statistic counter is of a given value.
   * Returns the value to assist in chaining,
   * @param statistic statistic to check
   * @param expected expected value
   * @return the expected value.
   */
  protected long verifyCounter(final Statistic statistic,
      final long expected) {
    IOStatisticAssertions.assertThatStatisticCounter(
        ioStatistics,
        statistic.getSymbol())
        .isEqualTo(expected);
    return expected;
  }

  /**
   * Create and switch to a span.
   * @return a span
   */
  protected AuditSpanS3A span() throws IOException {
    AuditSpanS3A span = manager.createSpan(OPERATION, PATH_1, PATH_2);
    assertThat(span)
        .matches(AuditSpan::isValidSpan);
    return span;
  }

  /**
   * Assert the map contains the expected (key, value).
   * @param params map of params
   * @param key key
   * @param expected expected value.
   */
  protected void assertMapContains(final Map<String, String> params,
      final String key, final String expected) {
    assertThat(params.get(key))
        .describedAs(key)
        .isEqualTo(expected);
  }

  /**
   * Assert the map does not contain the key, i.e, it is null.
   * @param params map of params
   * @param key key
   */
  protected void assertMapNotContains(final Map<String, String> params, final String key) {
    assertThat(params.get(key))
            .describedAs(key)
            .isNull();
  }

  /**
   * Create head request for bulk delete and pass it through beforeExecution of the manager.
   *
   * @param keys keys to be provided in the bulk delete request.
   * @return a processed request.
   */
  protected DeleteObjectsRequest headForBulkDelete(String... keys) {
    if (keys == null || keys.length == 0) {
      return null;
    }
    List<DeleteObjectsRequest.KeyVersion> keysToDelete = Arrays
        .stream(keys)
        .map(DeleteObjectsRequest.KeyVersion::new)
        .collect(Collectors.toList());
    return manager.beforeExecution(requestFactory.newBulkDeleteRequest(keysToDelete));
  }

}
