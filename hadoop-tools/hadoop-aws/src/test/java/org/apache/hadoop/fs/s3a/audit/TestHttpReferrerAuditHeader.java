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
import java.net.URISyntaxException;
import java.util.Map;
import java.util.regex.Matcher;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.audit.CommonAuditContext;
import org.apache.hadoop.fs.store.audit.HttpReferrerAuditHeader;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.audit.AuditConstants.DELETE_KEYS_SIZE;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.loggingAuditConfig;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REFERRER_HEADER_FILTER;
import static org.apache.hadoop.fs.s3a.audit.S3LogParser.*;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.HEADER_REFERRER;
import static org.apache.hadoop.fs.store.audit.HttpReferrerAuditHeader.maybeStripWrappedQuotes;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_FILESYSTEM_ID;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_ID;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_OP;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PATH;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PATH2;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PRINCIPAL;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_RANGE;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_THREAD0;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_THREAD1;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for referrer audit header generation/parsing.
 */
public class TestHttpReferrerAuditHeader extends AbstractAuditingTest {

  /**
   * Logging.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHttpReferrerAuditHeader.class);

  private LoggingAuditor auditor;

  @Before
  public void setup() throws Exception {
    super.setup();

    auditor = (LoggingAuditor) getManager().getAuditor();
  }

  /**
   * Create the config from {@link AuditTestSupport#loggingAuditConfig()}
   * and patch in filtering for fields x1, x2, x3.
   * @return a logging configuration.
   */
  protected Configuration createConfig() {
    final Configuration conf = loggingAuditConfig();
    conf.set(REFERRER_HEADER_FILTER, "x1, x2, x3");
    return conf;
  }

  /**
   * This verifies that passing a request through the audit manager
   * causes the http referrer header to be added, that it can
   * be split to query parameters, and that those parameters match
   * those of the active wrapped span.
   */
  @Test
  public void testHttpReferrerPatchesTheRequest() throws Throwable {
    AuditSpan span = span();
    long ts = span.getTimestamp();
    GetObjectMetadataRequest request = head();
    Map<String, String> headers
        = request.getCustomRequestHeaders();
    assertThat(headers)
        .describedAs("Custom headers")
        .containsKey(HEADER_REFERRER);
    String header = headers.get(HEADER_REFERRER);
    LOG.info("Header is {}", header);
    Map<String, String> params
        = HttpReferrerAuditHeader.extractQueryParameters(header);
    final String threadId = CommonAuditContext.currentThreadID();
    compareCommonHeaders(params, PATH_1, PATH_2, threadId, span);
    assertThat(span.getTimestamp())
        .describedAs("Timestamp of " + span)
        .isEqualTo(ts);
    assertMapNotContains(params, PARAM_RANGE);

    assertMapContains(params, PARAM_TIMESTAMP,
        Long.toString(ts));
  }

  /**
   * Test that a header with complext paths including spaces
   * and colons can be converted to a URI and back again
   * without the path getting corrupted.
   */
  @Test
  public void testHeaderComplexPaths() throws Throwable {
    String p1 = "s3a://dotted.bucket/path: value/subdir";
    String p2 = "s3a://key/";
    AuditSpan span = getManager().createSpan(OPERATION, p1, p2);
    long ts = span.getTimestamp();
    Map<String, String> params = issueRequestAndExtractParameters();
    final String threadId = CommonAuditContext.currentThreadID();
    compareCommonHeaders(params, p1, p2, threadId, span);
    assertThat(span.getTimestamp())
        .describedAs("Timestamp of " + span)
        .isEqualTo(ts);

    assertMapContains(params, PARAM_TIMESTAMP,
        Long.toString(ts));
  }

  /**
   * Issue a request, then get the header field and parse it to the parameter.
   * @return map of query params on the referrer header.
   * @throws URISyntaxException failure to parse the header as a URI.
   */
  private Map<String, String> issueRequestAndExtractParameters()
      throws URISyntaxException {
    head();
    return HttpReferrerAuditHeader.extractQueryParameters(
        auditor.getLastHeader());
  }


  /**
   * Test that headers are filtered out if configured.
   */
  @Test
  public void testHeaderFiltering() throws Throwable {
    // add two attributes, x2 will be filtered.
    AuditSpan span = getManager().createSpan(OPERATION, null, null);
    auditor.addAttribute("x0", "x0");
    auditor.addAttribute("x2", "x2");
    final Map<String, String> params
        = issueRequestAndExtractParameters();
    assertThat(params)
        .doesNotContainKey("x2");

  }

  /**
   * A real log entry.
   * This is derived from a real log entry on a test run.
   * If this needs to be updated, please do it from a real log.
   * Splitting this up across lines has a tendency to break things, so
   * be careful making changes.
   */
  public static final String SAMPLE_LOG_ENTRY =
      "183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4000000"
          + " bucket-london"
          + " [13/May/2021:11:26:06 +0000]"
          + " 109.157.171.174"
          + " arn:aws:iam::152813717700:user/dev"
          + " M7ZB7C4RTKXJKTM9"
          + " REST.PUT.OBJECT"
          + " fork-0001/test/testParseBrokenCSVFile"
          + " \"PUT /fork-0001/test/testParseBrokenCSVFile HTTP/1.1\""
          + " 200"
          + " -"
          + " -"
          + " 794"
          + " 55"
          + " 17"
          + " \"https://audit.example.org/hadoop/1/op_create/"
          + "e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278/"
          + "?op=op_create"
          + "&p1=fork-0001/test/testParseBrokenCSVFile"
          + "&pr=alice"
          + "&ps=2eac5a04-2153-48db-896a-09bc9a2fd132"
          + "&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154"
          + "&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156&"
          + "ts=1620905165700\""
          + " \"Hadoop 3.4.0-SNAPSHOT, java/1.8.0_282 vendor/AdoptOpenJDK\""
          + " -"
          + " TrIqtEYGWAwvu0h1N9WJKyoqM0TyHUaY+ZZBwP2yNf2qQp1Z/0="
          + " SigV4"
          + " ECDHE-RSA-AES128-GCM-SHA256"
          + " AuthHeader"
          + " bucket-london.s3.eu-west-2.amazonaws.com"
          + " TLSv1.2";

  private static final String DESCRIPTION = String.format(
      "log entry %s split by %s", SAMPLE_LOG_ENTRY,
      LOG_ENTRY_PATTERN);

  /**
   * Match the log entry and validate the results.
   */
  @Test
  public void testMatchAWSLogEntry() throws Throwable {

    LOG.info("Matcher pattern is\n'{}'", LOG_ENTRY_PATTERN);
    LOG.info("Log entry is\n'{}'", SAMPLE_LOG_ENTRY);
    final Matcher matcher = LOG_ENTRY_PATTERN.matcher(SAMPLE_LOG_ENTRY);

    // match the pattern against the entire log entry.
    assertThat(matcher.matches())
        .describedAs("matches() " + DESCRIPTION)
        .isTrue();
    final int groupCount = matcher.groupCount();
    assertThat(groupCount)
        .describedAs("Group count of " + DESCRIPTION)
        .isGreaterThanOrEqualTo(AWS_LOG_REGEXP_GROUPS.size());

    // now go through the groups

    for (String name : AWS_LOG_REGEXP_GROUPS) {
      try {
        final String group = matcher.group(name);
        LOG.info("[{}]: '{}'", name, group);
      } catch (IllegalStateException e) {
        // group failure
        throw new AssertionError("No match for group <" + name + ">: "
            + e, e);
      }
    }
    // if you print out the groups as integers, there is duplicate matching
    // for some fields. Why?
    for (int i = 1; i <= groupCount; i++) {
      try {
        final String group = matcher.group(i);
        LOG.info("[{}]: '{}'", i, group);
      } catch (IllegalStateException e) {
        // group failure
        throw new AssertionError("No match for group " + i
            +": "+ e, e);
      }
    }

    // verb
    assertThat(nonBlankGroup(matcher, VERB_GROUP))
        .describedAs("HTTP Verb")
        .isEqualTo(S3LogVerbs.PUT);

    // referrer
    final String referrer = nonBlankGroup(matcher, REFERRER_GROUP);
    Map<String, String> params
        = HttpReferrerAuditHeader.extractQueryParameters(referrer);
    LOG.info("Parsed referrer");
    for (Map.Entry<String, String> entry : params.entrySet()) {
      LOG.info("{} = \"{}\"", entry.getKey(), entry.getValue());
    }
  }

  /**
   * Get a group entry which must be non-blank.
   * @param matcher matcher
   * @param group group name
   * @return value
   */
  private String nonBlankGroup(final Matcher matcher,
      final String group) {
    final String g = matcher.group(group);
    assertThat(g)
        .describedAs("Value of group %s", group)
        .isNotBlank();
    return g;
  }

  /**
   * Verify the header quote stripping works.
   */
  @Test
  public void testStripWrappedQuotes() throws Throwable {
    expectStrippedField("", "");
    expectStrippedField("\"UA\"", "UA");
    expectStrippedField("\"\"\"\"", "");
    expectStrippedField("\"\"\"b\"", "b");
  }

  /**
   * Verify that correct range is getting published in header.
   */
  @Test
  public void testGetObjectRange() throws Throwable {
    AuditSpan span = span();
    GetObjectRequest request = get(getObjectRequest -> getObjectRequest.setRange(100, 200));
    Map<String, String> headers
            = request.getCustomRequestHeaders();
    assertThat(headers)
            .describedAs("Custom headers")
            .containsKey(HEADER_REFERRER);
    String header = headers.get(HEADER_REFERRER);
    LOG.info("Header is {}", header);
    Map<String, String> params
            = HttpReferrerAuditHeader.extractQueryParameters(header);
    assertMapContains(params, PARAM_RANGE, "100-200");
  }

  /**
   * Verify that no range is getting added to the header in request without range.
   */
  @Test
  public void testGetObjectWithoutRange() throws Throwable {
    AuditSpan span = span();
    GetObjectRequest request = get(getObjectRequest -> {});
    Map<String, String> headers
        = request.getCustomRequestHeaders();
    assertThat(headers)
        .describedAs("Custom headers")
        .containsKey(HEADER_REFERRER);
    String header = headers.get(HEADER_REFERRER);
    LOG.info("Header is {}", header);
    Map<String, String> params
        = HttpReferrerAuditHeader.extractQueryParameters(header);
    assertMapNotContains(params, PARAM_RANGE);
  }

  @Test
  public void testHttpReferrerForBulkDelete() throws Throwable {
    AuditSpan span = span();
    long ts = span.getTimestamp();
    DeleteObjectsRequest request = headForBulkDelete(
        "key_01",
        "key_02",
        "key_03");
    Map<String, String> headers
        = request.getCustomRequestHeaders();
    assertThat(headers)
        .describedAs("Custom headers")
        .containsKey(HEADER_REFERRER);
    String header = headers.get(HEADER_REFERRER);
    LOG.info("Header is {}", header);
    Map<String, String> params
        = HttpReferrerAuditHeader.extractQueryParameters(header);
    final String threadId = CommonAuditContext.currentThreadID();
    compareCommonHeaders(params, PATH_1, PATH_2, threadId, span);
    assertMapContains(params, DELETE_KEYS_SIZE, "3");
    assertThat(span.getTimestamp())
        .describedAs("Timestamp of " + span)
        .isEqualTo(ts);
    assertMapNotContains(params, PARAM_RANGE);

    assertMapContains(params, PARAM_TIMESTAMP,
        Long.toString(ts));
  }

  /**
   * Utility to compare common params from the referer header.
   *
   * @param params map of params extracted from the header.
   * @param path1 first path.
   * @param path2 second path.
   * @param threadID thread id.
   * @param span audit span object.
   * @throws IOException if login fails and/or current user cannot be retrieved.
   */
  private void compareCommonHeaders(final Map<String, String> params,
      final String path1,
      final String path2,
      final String threadID,
      final AuditSpan span) throws IOException {
    assertMapContains(params, PARAM_PRINCIPAL,
        UserGroupInformation.getCurrentUser().getUserName());
    assertMapContains(params, PARAM_FILESYSTEM_ID,
        auditor.getAuditorId());
    assertMapContains(params, PARAM_OP, OPERATION);
    assertMapContains(params, PARAM_PATH, path1);
    assertMapContains(params, PARAM_PATH2, path2);
    assertMapContains(params, PARAM_THREAD0, threadID);
    assertMapContains(params, PARAM_THREAD1, threadID);
    assertMapContains(params, PARAM_ID, span.getSpanId());
  }

  /**
   * Expect a field with quote stripping to match the expected value.
   * @param str string to strip
   * @param ex expected value.
   */
  private void expectStrippedField(final String str,
      final String ex) {
    assertThat(maybeStripWrappedQuotes(str))
        .describedAs("Stripped <%s>", str)
        .isEqualTo(ex);
  }
}