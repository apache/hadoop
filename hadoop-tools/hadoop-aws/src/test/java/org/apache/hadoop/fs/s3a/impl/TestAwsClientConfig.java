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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.Lists;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.DecodeAuthorizationMessageRequest;
import software.amazon.awssdk.services.sts.model.GetSessionTokenRequest;

import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_S3;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_STS;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_ACQUISITION_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_IDLE_TIME;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_TTL;
import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_HEADERS_S3;
import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_HEADERS_STS;
import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_REQUEST_HEADERS_S3_PREFIX;
import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_REQUEST_HEADERS_STS_PREFIX;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_IDLE_TIME_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_TTL_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ESTABLISH_TIMEOUT_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_REQUEST_TIMEOUT_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SOCKET_TIMEOUT_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.ESTABLISH_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MINIMUM_NETWORK_OPERATION_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.s3a.impl.AWSClientConfig.createApiConnectionSettings;
import static org.apache.hadoop.fs.s3a.impl.AWSClientConfig.createClientConfigBuilder;
import static org.apache.hadoop.fs.s3a.impl.AWSClientConfig.createConnectionSettings;
import static org.apache.hadoop.fs.s3a.impl.ConfigurationHelper.enforceMinimumDuration;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AWSClientConfig}.
 * These may play with the config timeout settings, so reset the timeouts
 * during teardown.
 * For isolation from any site settings, the tests create configurations
 * without loading of defaut/site XML files.
 */
public class TestAwsClientConfig extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestAwsClientConfig.class);

  @AfterEach
  public void teardown() throws Exception {
    AWSClientConfig.resetMinimumOperationDuration();
  }

  /**
   * Create a new empty configuration.
   * @return configuration.
   */
  private Configuration conf() {
    return new Configuration(false);
  }

  /**
   * Innermost duration enforcement, which is not applied if
   * the minimum value is null.
   */
  @Test
  public void testEnforceMinDuration() {
    final Duration s10 = Duration.ofSeconds(10);
    final Duration s1 = Duration.ofSeconds(1);

    assertThat(enforceMinimumDuration("key", s1, s10))
        .describedAs("10s")
        .isEqualTo(s10);

    // and a null check
    assertThat(enforceMinimumDuration("key",
        s1, null))
        .describedAs("10s")
        .isEqualTo(s1);
  }

  /**
   * When loading a connection settings from an empty configuration, the
   * correct default values are loaded.
   */
  @Test
  public void testLoadUnsetValues() {
    final AWSClientConfig.ConnectionSettings conn = createConnectionSettings(conf());
    assertDuration(CONNECTION_ACQUISITION_TIMEOUT, DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_DURATION,
        conn.getAcquisitionTimeout());
    assertDuration(CONNECTION_TTL, DEFAULT_CONNECTION_TTL_DURATION,
        conn.getConnectionTTL());
    assertDuration(CONNECTION_IDLE_TIME, DEFAULT_CONNECTION_IDLE_TIME_DURATION,
        conn.getMaxIdleTime());
    assertDuration(ESTABLISH_TIMEOUT, DEFAULT_ESTABLISH_TIMEOUT_DURATION,
        conn.getEstablishTimeout());
    assertDuration(SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT_DURATION,
        conn.getSocketTimeout());
    assertThat(conn.getMaxConnections())
        .describedAs(MAXIMUM_CONNECTIONS)
        .isEqualTo(DEFAULT_MAXIMUM_CONNECTIONS);
    assertThat(conn.isKeepAlive())
        .describedAs(CONNECTION_KEEPALIVE)
        .isEqualTo(DEFAULT_CONNECTION_KEEPALIVE);
  }

  /**
   * If we set a minimum duration that is bigger than the configured value,
   * the minimum value wins.
   * Some options have a minimum value of zero.
   */
  @Test
  public void testMinimumDurationWins() {

    final Configuration conf = conf();
    setOptionsToValue("1s", conf,
        CONNECTION_ACQUISITION_TIMEOUT,
        CONNECTION_TTL,
        CONNECTION_IDLE_TIME,
        ESTABLISH_TIMEOUT,
        SOCKET_TIMEOUT);
    final AWSClientConfig.ConnectionSettings conn = createConnectionSettings(conf);
    LOG.info("Connection settings: {}", conn);
    assertDuration(CONNECTION_ACQUISITION_TIMEOUT, MINIMUM_NETWORK_OPERATION_DURATION,
        conn.getAcquisitionTimeout());

    assertDuration(ESTABLISH_TIMEOUT, MINIMUM_NETWORK_OPERATION_DURATION,
        conn.getEstablishTimeout());
    assertDuration(SOCKET_TIMEOUT, MINIMUM_NETWORK_OPERATION_DURATION,
        conn.getSocketTimeout());

    // those options with a minimum of zero
    final Duration s1 = Duration.ofSeconds(1);
    assertDuration(CONNECTION_TTL, s1, conn.getConnectionTTL());
    assertDuration(CONNECTION_IDLE_TIME, s1, conn.getMaxIdleTime());
  }

  /**
   * Assert that a a duration has the expected value.
   * @param name option name for assertion text
   * @param expected expected duration
   * @param actual actual duration
   */
  private void assertDuration(String name, Duration expected, Duration actual) {
    assertThat(actual)
        .describedAs("Duration of %s", name)
        .isEqualTo(expected);
  }

  /**
   * Test {@link AWSClientConfig#createApiConnectionSettings(Configuration)}.
   */
  @Test
  public void testCreateApiConnectionSettings() {
    final Configuration conf = conf();
    conf.set(REQUEST_TIMEOUT, "1h");
    final AWSClientConfig.ClientSettings settings =
        createApiConnectionSettings(conf);
    assertThat(settings.getApiCallTimeout())
        .describedAs("%s in %s", REQUEST_TIMEOUT, settings)
        .isEqualTo(Duration.ofHours(1));
  }
  /**
   * Verify that the timeout from {@link org.apache.hadoop.fs.s3a.Constants#DEFAULT_REQUEST_TIMEOUT_DURATION}
   * makes it all the way through and that nothing in in core-default or core-site is setting it.
   * This test will fail if someone does set it in core-site.xml
   */
  @Test
  public void testCreateApiConnectionSettingsDefault() {
    final Configuration conf = new Configuration();
    assertThat(conf.get(REQUEST_TIMEOUT))
        .describedAs("Request timeout %s", REQUEST_TIMEOUT)
        .isNull();

    assertDuration(REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT_DURATION,
        createApiConnectionSettings(conf).getApiCallTimeout());
  }

  /**
   * Set a list of keys to the same value.
   * @param value value to set
   * @param conf configuration to patch
   * @param keys keys
   */
  private void setOptionsToValue(String value, Configuration conf, String... keys) {
    Arrays.stream(keys).forEach(key -> conf.set(key, value));
  }

  /**
   * if {@link org.apache.hadoop.fs.s3a.Constants#CUSTOM_HEADERS_STS} is set,
   * verify that returned client configuration has desired headers set.
   */
  @Test
  public void testInitRequestHeadersForSTS() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(CUSTOM_HEADERS_STS, "header1=value1;value2,header2=value3");
    conf.set(CUSTOM_REQUEST_HEADERS_STS_PREFIX + "GetSessionTokenRequest", "header3=value4;value5,header4=value6");
    conf.set(CUSTOM_REQUEST_HEADERS_STS_PREFIX + "assumerolerequest", "header5=value7");

    assertThat(conf.get(CUSTOM_HEADERS_S3))
            .describedAs("Custom client headers for s3 %s", CUSTOM_HEADERS_S3)
            .isNull();
    assertThat(conf.getPropsWithPrefix(CUSTOM_REQUEST_HEADERS_S3_PREFIX))
            .describedAs("Custom per-request client headers for s3 %s", CUSTOM_REQUEST_HEADERS_S3_PREFIX)
            .isEmpty();

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
            .headers().size())
        .describedAs("Count of S3 client headers")
        .isEqualTo(0);
    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
            .executionInterceptors().stream()
            .filter(ei -> ei instanceof AddRequestHeaderInterceptor)
            .count())
        .describedAs("Count of request header interceptors of S3 client")
        .isEqualTo(0);

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_STS)
            .headers().size())
        .describedAs("Count of STS client headers")
        .isEqualTo(2);

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_STS)
            .headers().get("header1"))
        .describedAs("STS client 'header1' header value")
        .isEqualTo(Lists.newArrayList("value1", "value2"));

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_STS)
            .headers().get("header2"))
        .describedAs("STS client 'header2' header value")
        .isEqualTo(Lists.newArrayList("value3"));

    List<AddRequestHeaderInterceptor> interceptors =
            createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_STS)
                .executionInterceptors().stream()
                .filter(ei -> ei instanceof AddRequestHeaderInterceptor)
                .map(ie -> (AddRequestHeaderInterceptor) ie)
                .toList();

    assertThat(interceptors.size())
        .describedAs("Count of request header interceptors of STS client")
        .isEqualTo(1);

    AddRequestHeaderInterceptor interceptor = interceptors.get(0);

    SdkRequest request = DecodeAuthorizationMessageRequest.builder().build();
    Context.ModifyRequest modifyRequest = InterceptorContext.builder().request(request).build();
    SdkRequest modifiedRequest = interceptor.modifyRequest(modifyRequest, null);
    assertThat(modifiedRequest.overrideConfiguration().isPresent())
            .describedAs("STS list request has override configuration")
            .isFalse();

    SdkRequest getRequest = GetSessionTokenRequest.builder().build();
    Context.ModifyRequest modifyGetRequest = InterceptorContext.builder().request(getRequest).build();
    SdkRequest modifiedGetRequest = interceptor.modifyRequest(modifyGetRequest, null);
    assertThat(modifiedGetRequest.overrideConfiguration().isPresent())
        .describedAs("STS list request has override configuration")
        .isTrue();
    assertThat(modifiedGetRequest.overrideConfiguration().get().headers().keySet())
        .describedAs("STS client request headers")
        .isEqualTo(new HashSet<>(Lists.newArrayList("header3", "header4")));
    assertThat(modifiedGetRequest.overrideConfiguration().get().headers().get("header3"))
        .describedAs("STS client request 'header3' header value")
        .isEqualTo(Lists.newArrayList("value4", "value5"));
    assertThat(modifiedGetRequest.overrideConfiguration().get().headers().get("header4"))
        .describedAs("STS client request 'header4' header value")
        .isEqualTo(Lists.newArrayList("value6"));

    SdkRequest assumeRequest = AssumeRoleRequest.builder().build();
    Context.ModifyRequest modifyAssumeRequest = InterceptorContext.builder().request(assumeRequest).build();
    SdkRequest modifiedAssumeRequest = interceptor.modifyRequest(modifyAssumeRequest, null);
    assertThat(modifiedAssumeRequest.overrideConfiguration().isPresent())
        .describedAs("STS delete request has override configuration")
        .isTrue();
    assertThat(modifiedAssumeRequest.overrideConfiguration().get().headers().keySet())
        .describedAs("STS client request headers")
        .isEqualTo(new HashSet<>(Lists.newArrayList("header5")));
    assertThat(modifiedAssumeRequest.overrideConfiguration().get().headers().get("header5"))
        .describedAs("STS client request 'header3' header value")
        .isEqualTo(Lists.newArrayList("value7"));
  }

  /**
   * if {@link org.apache.hadoop.fs.s3a.Constants#CUSTOM_HEADERS_S3} is set,
   * verify that returned client configuration has desired headers set.
   */
  @Test
  public void testInitRequestHeadersForS3() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(CUSTOM_HEADERS_S3, "header1=value1;value2,header2=value3");
    conf.set(CUSTOM_REQUEST_HEADERS_S3_PREFIX + "ListObjectsV2Request", "header3=value4;value5,header4=value6");
    conf.set(CUSTOM_REQUEST_HEADERS_S3_PREFIX + "deleteobjectrequest", "header5=value7");

    assertThat(conf.get(CUSTOM_HEADERS_STS))
            .describedAs("Custom client headers for STS %s", CUSTOM_HEADERS_STS)
            .isNull();
    assertThat(conf.getPropsWithPrefix(CUSTOM_REQUEST_HEADERS_STS_PREFIX))
            .describedAs("Custom per-request client headers for STS %s", CUSTOM_REQUEST_HEADERS_STS_PREFIX)
            .isEmpty();

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_STS)
            .headers().size())
        .describedAs("Count of STS client headers")
        .isEqualTo(0);
    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_STS)
            .executionInterceptors().stream()
            .filter(ei -> ei instanceof AddRequestHeaderInterceptor)
            .count())
            .describedAs("Count of request header interceptors of STS client")
            .isEqualTo(0);

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
            .headers().size())
        .describedAs("Count of S3 client headers")
        .isEqualTo(2);

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
            .headers().get("header1"))
        .describedAs("S3 client 'header1' header value")
        .isEqualTo(Lists.newArrayList("value1", "value2"));

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
            .headers().get("header2"))
        .describedAs("S3 client 'header2' header value")
        .isEqualTo(Lists.newArrayList("value3"));

    List<AddRequestHeaderInterceptor> interceptors =
            createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
                    .executionInterceptors().stream()
                    .filter(ei -> ei instanceof AddRequestHeaderInterceptor)
                    .map(ie -> (AddRequestHeaderInterceptor) ie)
                    .toList();

    assertThat(interceptors.size())
        .describedAs("Count of request header interceptors of S3 client")
        .isEqualTo(1);

    AddRequestHeaderInterceptor interceptor = interceptors.get(0);

    SdkRequest request = CreateBucketRequest.builder().build();
    Context.ModifyRequest modifyRequest = InterceptorContext.builder().request(request).build();
    SdkRequest modifiedRequest = interceptor.modifyRequest(modifyRequest, null);
    assertThat(modifiedRequest.overrideConfiguration().isPresent())
            .describedAs("S3 list request has override configuration")
            .isFalse();

    SdkRequest listRequest = ListObjectsV2Request.builder().build();
    Context.ModifyRequest modifyListRequest = InterceptorContext.builder().request(listRequest).build();
    SdkRequest modifiedListRequest = interceptor.modifyRequest(modifyListRequest, null);
    assertThat(modifiedListRequest.overrideConfiguration().isPresent())
        .describedAs("S3 list request has override configuration")
        .isTrue();
    assertThat(modifiedListRequest.overrideConfiguration().get().headers().keySet())
        .describedAs("S3 client request headers")
        .isEqualTo(new HashSet<>(Lists.newArrayList("header3", "header4")));
    assertThat(modifiedListRequest.overrideConfiguration().get().headers().get("header3"))
        .describedAs("S3 client request 'header3' header value")
        .isEqualTo(Lists.newArrayList("value4", "value5"));
    assertThat(modifiedListRequest.overrideConfiguration().get().headers().get("header4"))
        .describedAs("S3 client request 'header4' header value")
        .isEqualTo(Lists.newArrayList("value6"));

    SdkRequest deleteRequest = DeleteObjectRequest.builder().build();
    Context.ModifyRequest modifyDeleteRequest = InterceptorContext.builder().request(deleteRequest).build();
    SdkRequest modifiedDeleteRequest = interceptor.modifyRequest(modifyDeleteRequest, null);
    assertThat(modifiedDeleteRequest.overrideConfiguration().isPresent())
        .describedAs("S3 delete request has override configuration")
        .isTrue();
    assertThat(modifiedDeleteRequest.overrideConfiguration().get().headers().keySet())
        .describedAs("S3 client request headers")
        .isEqualTo(new HashSet<>(Lists.newArrayList("header5")));
    assertThat(modifiedDeleteRequest.overrideConfiguration().get().headers().get("header5"))
          .describedAs("S3 client request 'header3' header value")
          .isEqualTo(Lists.newArrayList("value7"));
  }

  /**
   * if {@link org.apache.hadoop.fs.s3a.Constants#CUSTOM_HEADERS_S3} is set,
   * verify that returned client configuration has desired headers set with
   * whitespaces trimmed for headers and values.
   */
  @Test
  public void testInitRequestHeadersForS3WithWhitespace() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(CUSTOM_HEADERS_S3, "  header1 =  value1 ;  value2 ,   header2= value3  ");

    assertThat(conf.get(CUSTOM_HEADERS_STS))
            .describedAs("Custom client headers for STS %s", CUSTOM_HEADERS_STS)
            .isNull();

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_STS)
                    .headers().size())
            .describedAs("Count of STS client headers")
            .isEqualTo(0);

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
                    .headers().size())
            .describedAs("Count of S3 client headers")
            .isEqualTo(2);

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
                    .headers().get("header1"))
            .describedAs("S3 client 'header1' header value")
            .isEqualTo(Lists.newArrayList("value1", "value2"));

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
                    .headers().get("header2"))
            .describedAs("S3 client 'header2' header value")
            .isEqualTo(Lists.newArrayList("value3"));
  }

  /**
   * if {@link org.apache.hadoop.fs.s3a.Constants#CUSTOM_HEADERS_S3} is set with duplicate values,
   * verify that returned client configuration has desired headers with both values.
   */
  @Test
  public void testInitRequestHeadersForS3WithDuplicateValues() throws IOException {
    Configuration conf = new Configuration();
    conf.set(CUSTOM_HEADERS_S3, "header1=duplicate;duplicate");

    assertThat(createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3)
                    .headers().get("header1"))
            .describedAs("S3 client 'header1' header value")
            .isEqualTo(Lists.newArrayList("duplicate", "duplicate"));
  }
}
