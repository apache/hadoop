/**
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
package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.DOT_AUTH_URL;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.DOT_LOCATION_AWARE;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.DOT_PASSWORD;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.DOT_TENANT;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.DOT_USERNAME;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.SWIFT_BLOCKSIZE;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.SWIFT_CONNECTION_TIMEOUT;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.SWIFT_PARTITION_SIZE;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.SWIFT_PROXY_HOST_PROPERTY;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.SWIFT_PROXY_PORT_PROPERTY;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.SWIFT_RETRY_COUNT;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.SWIFT_SERVICE_PREFIX;

/**
 * Test the swift service-specific configuration binding features
 */
public class TestSwiftConfig extends Assert {


  public static final String SERVICE = "openstack";

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testEmptyUrl() throws Exception {
    final Configuration configuration = new Configuration();

    set(configuration, DOT_TENANT, "tenant");
    set(configuration, DOT_USERNAME, "username");
    set(configuration, DOT_PASSWORD, "password");
    mkInstance(configuration);
  }

@Test
  public void testEmptyTenant() throws Exception {
    final Configuration configuration = new Configuration();
    set(configuration, DOT_AUTH_URL, "http://localhost:8080");
    set(configuration, DOT_USERNAME, "username");
    set(configuration, DOT_PASSWORD, "password");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testEmptyUsername() throws Exception {
    final Configuration configuration = new Configuration();
    set(configuration, DOT_AUTH_URL, "http://localhost:8080");
    set(configuration, DOT_TENANT, "tenant");
    set(configuration, DOT_PASSWORD, "password");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testEmptyPassword() throws Exception {
    final Configuration configuration = new Configuration();
    set(configuration, DOT_AUTH_URL, "http://localhost:8080");
    set(configuration, DOT_TENANT, "tenant");
    set(configuration, DOT_USERNAME, "username");
    mkInstance(configuration);
  }

  @Test
  public void testGoodRetryCount() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_RETRY_COUNT, "3");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testBadRetryCount() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_RETRY_COUNT, "three");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testBadConnectTimeout() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_CONNECTION_TIMEOUT, "three");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testZeroBlocksize() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_BLOCKSIZE, "0");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testNegativeBlocksize() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_BLOCKSIZE, "-1");
    mkInstance(configuration);
  }

  @Test
  public void testPositiveBlocksize() throws Exception {
    final Configuration configuration = createCoreConfig();
    int size = 127;
    configuration.set(SWIFT_BLOCKSIZE, Integer.toString(size));
    SwiftRestClient restClient = mkInstance(configuration);
    assertEquals(size, restClient.getBlocksizeKB());
  }

  @Test
  public void testLocationAwareTruePropagates() throws Exception {
    final Configuration configuration = createCoreConfig();
    set(configuration, DOT_LOCATION_AWARE, "true");
    SwiftRestClient restClient = mkInstance(configuration);
    assertTrue(restClient.isLocationAware());
  }

  @Test
  public void testLocationAwareFalsePropagates() throws Exception {
    final Configuration configuration = createCoreConfig();
    set(configuration, DOT_LOCATION_AWARE, "false");
    SwiftRestClient restClient = mkInstance(configuration);
    assertFalse(restClient.isLocationAware());
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testNegativePartsize() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_PARTITION_SIZE, "-1");
    SwiftRestClient restClient = mkInstance(configuration);
  }

  @Test
  public void testPositivePartsize() throws Exception {
    final Configuration configuration = createCoreConfig();
    int size = 127;
    configuration.set(SWIFT_PARTITION_SIZE, Integer.toString(size));
    SwiftRestClient restClient = mkInstance(configuration);
    assertEquals(size, restClient.getPartSizeKB());
  }

  @Test
  public void testProxyData() throws Exception {
    final Configuration configuration = createCoreConfig();
    String proxy="web-proxy";
    int port = 8088;
    configuration.set(SWIFT_PROXY_HOST_PROPERTY, proxy);
    configuration.set(SWIFT_PROXY_PORT_PROPERTY, Integer.toString(port));
    SwiftRestClient restClient = mkInstance(configuration);
    assertEquals(proxy, restClient.getProxyHost());
    assertEquals(port, restClient.getProxyPort());
  }

  private Configuration createCoreConfig() {
    final Configuration configuration = new Configuration();
    set(configuration, DOT_AUTH_URL, "http://localhost:8080");
    set(configuration, DOT_TENANT, "tenant");
    set(configuration, DOT_USERNAME, "username");
    set(configuration, DOT_PASSWORD, "password");
    return configuration;
  }

  private void set(Configuration configuration, String field, String value) {
    configuration.set(SWIFT_SERVICE_PREFIX + SERVICE + field, value);
  }

  private SwiftRestClient mkInstance(Configuration configuration) throws
          IOException,
          URISyntaxException {
    URI uri = new URI("swift://container.openstack/");
    return SwiftRestClient.getInstance(uri, configuration);
  }
}
