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

import java.time.Duration;
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_ACQUISITION_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_IDLE_TIME;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_TTL;
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
import static org.apache.hadoop.fs.s3a.impl.AWSClientConfig.createConnectionSettings;
import static org.apache.hadoop.fs.s3a.impl.ConfigurationHelper.enforceMinimumDuration;

/**
 * Unit tests for {@link AWSClientConfig}.
 * These may play with the config timeout settings, so reset the timeouts
 * during teardown.
 * For isolation from any site settings, the tests create configurations
 * without loading of defaut/site XML files.
 */
public class TestAwsClientConfig extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestAwsClientConfig.class);

  @After
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

    Assertions.assertThat(enforceMinimumDuration("key", s1, s10))
        .describedAs("10s")
        .isEqualTo(s10);

    // and a null check
    Assertions.assertThat(enforceMinimumDuration("key",
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
    Assertions.assertThat(conn.getMaxConnections())
        .describedAs(MAXIMUM_CONNECTIONS)
        .isEqualTo(DEFAULT_MAXIMUM_CONNECTIONS);
    Assertions.assertThat(conn.isKeepAlive())
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
    Assertions.assertThat(actual)
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
    Assertions.assertThat(settings.getApiCallTimeout())
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
    Assertions.assertThat(conf.get(REQUEST_TIMEOUT))
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
}
