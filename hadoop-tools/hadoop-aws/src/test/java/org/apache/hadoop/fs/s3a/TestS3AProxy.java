/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.PROXY_HOST;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_PORT;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_SECURED;

/**
 * Tests to verify {@link S3AUtils} translates the proxy configurations
 * are set correctly to Client configurations which are later used to construct
 * the proxy in AWS SDK.
 */
public class TestS3AProxy extends AbstractHadoopTestBase {

  /**
   * Verify Http proxy protocol.
   */
  @Test
  public void testProxyHttp() throws IOException {
    Configuration proxyConfigForHttp = createProxyConfig(false);
    verifyProxy(proxyConfigForHttp, false);
  }

  /**
   * Verify Https proxy protocol.
   */
  @Test
  public void testProxyHttps() throws IOException {
    Configuration proxyConfigForHttps = createProxyConfig(true);
    verifyProxy(proxyConfigForHttps, true);
  }

  /**
   * Verify default proxy protocol.
   */
  @Test
  public void testProxyDefault() throws IOException {
    Configuration proxyConfigDefault = new Configuration();
    proxyConfigDefault.set(PROXY_HOST, "testProxyDefault");
    verifyProxy(proxyConfigDefault, false);
  }

  /**
   * Assert that the configuration set for a proxy gets translated to Client
   * configuration with the correct protocol to be used by AWS SDK.
   * @param proxyConfig Configuration used to set the proxy configs.
   * @param isExpectedSecured What is the expected protocol for the proxy to
   *                          be? true for https, and false for http.
   * @throws IOException
   */
  private void verifyProxy(Configuration proxyConfig,
      boolean isExpectedSecured)
      throws IOException {
    ProxyConfiguration config =
        AWSClientConfig.createProxyConfiguration(proxyConfig, "testBucket");
    ProxyConfiguration asyncConfig =
        AWSClientConfig.createProxyConfiguration(proxyConfig, "testBucket");
    Assertions.assertThat(config.scheme())
        .describedAs("Proxy protocol not as expected")
        .isEqualTo(isExpectedSecured ? "https" : "http");
    Assertions.assertThat(asyncConfig.scheme())
        .describedAs("Proxy protocol not as expected")
        .isEqualTo(isExpectedSecured ? "https" : "http");
  }

  /**
   * Create a configuration file with proxy configs.
   * @param isSecured Should the configured proxy be secured or not?
   * @return configuration.
   */
  private Configuration createProxyConfig(boolean isSecured) {
    Configuration conf = new Configuration();
    conf.set(PROXY_HOST, "testProxy");
    conf.set(PROXY_PORT, "1234");
    conf.setBoolean(PROXY_SECURED, isSecured);
    return conf;
  }
}
