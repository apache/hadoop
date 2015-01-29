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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.commons.lang.StringUtils;
import com.amazonaws.AmazonClientException;
import org.apache.hadoop.conf.Configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestS3AConfiguration {
  private Configuration conf;
  private S3AFileSystem fs;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AConfiguration.class);

  private static final String TEST_ENDPOINT = "test.fs.s3a.endpoint";

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  /**
   * Test if custom endpoint is picked up.
   * <p/>
   * The test expects TEST_ENDPOINT to be defined in the Configuration
   * describing the endpoint of the bucket to which TEST_FS_S3A_NAME points
   * (f.i. "s3-eu-west-1.amazonaws.com" if the bucket is located in Ireland).
   * Evidently, the bucket has to be hosted in the region denoted by the
   * endpoint for the test to succeed.
   * <p/>
   * More info and the list of endpoint identifiers:
   * http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
   *
   * @throws Exception
   */
  @Test
  public void TestEndpoint() throws Exception {
    conf = new Configuration();
    String endpoint = conf.getTrimmed(TEST_ENDPOINT, "");
    if (endpoint.isEmpty()) {
      LOG.warn("Custom endpoint test skipped as " + TEST_ENDPOINT + "config " +
          "setting was not detected");
    } else {
      conf.set(Constants.ENDPOINT, endpoint);
      fs = S3ATestUtils.createTestFileSystem(conf);
      AmazonS3Client s3 = fs.getAmazonS3Client();
      String endPointRegion = "";
      // Differentiate handling of "s3-" and "s3." based endpoint identifiers
      String[] endpointParts = StringUtils.split(endpoint, '.');
      if (endpointParts.length == 3) {
        endPointRegion = endpointParts[0].substring(3);
      } else if (endpointParts.length == 4) {
        endPointRegion = endpointParts[1];
      } else {
        fail("Unexpected endpoint");
      }
      assertEquals("Endpoint config setting and bucket location differ: ",
          endPointRegion, s3.getBucketLocation(fs.getUri().getHost()));
    }
  }

  @Test
  public void TestProxyConnection() throws Exception {
    conf = new Configuration();
    conf.setInt(Constants.MAX_ERROR_RETRIES, 2);
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.setInt(Constants.PROXY_PORT, 1);
    String proxy =
        conf.get(Constants.PROXY_HOST) + ":" + conf.get(Constants.PROXY_PORT);
    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      fail("Expected a connection error for proxy server at " + proxy);
    } catch (AmazonClientException e) {
      if (!e.getMessage().contains(proxy + " refused")) {
        throw e;
      }
    }
  }

  @Test
  public void TestProxyPortWithoutHost() throws Exception {
    conf = new Configuration();
    conf.setInt(Constants.MAX_ERROR_RETRIES, 2);
    conf.setInt(Constants.PROXY_PORT, 1);
    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      fail("Expected a proxy configuration error");
    } catch (IllegalArgumentException e) {
      String msg = e.toString();
      if (!msg.contains(Constants.PROXY_HOST) &&
          !msg.contains(Constants.PROXY_PORT)) {
        throw e;
      }
    }
  }

  @Test
  public void TestAutomaticProxyPortSelection() throws Exception {
    conf = new Configuration();
    conf.setInt(Constants.MAX_ERROR_RETRIES, 2);
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.set(Constants.SECURE_CONNECTIONS, "true");
    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      fail("Expected a connection error for proxy server");
    } catch (AmazonClientException e) {
      if (!e.getMessage().contains("443")) {
        throw e;
      }
    }
    conf.set(Constants.SECURE_CONNECTIONS, "false");
    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      fail("Expected a connection error for proxy server");
    } catch (AmazonClientException e) {
      if (!e.getMessage().contains("80")) {
        throw e;
      }
    }
  }

  @Test
  public void TestUsernameInconsistentWithPassword() throws Exception {
    conf = new Configuration();
    conf.setInt(Constants.MAX_ERROR_RETRIES, 2);
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.setInt(Constants.PROXY_PORT, 1);
    conf.set(Constants.PROXY_USERNAME, "user");
    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      fail("Expected a connection error for proxy server");
    } catch (IllegalArgumentException e) {
      String msg = e.toString();
      if (!msg.contains(Constants.PROXY_USERNAME) &&
          !msg.contains(Constants.PROXY_PASSWORD)) {
        throw e;
      }
    }
    conf = new Configuration();
    conf.setInt(Constants.MAX_ERROR_RETRIES, 2);
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.setInt(Constants.PROXY_PORT, 1);
    conf.set(Constants.PROXY_PASSWORD, "password");
    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      fail("Expected a connection error for proxy server");
    } catch (IllegalArgumentException e) {
      String msg = e.toString();
      if (!msg.contains(Constants.PROXY_USERNAME) &&
          !msg.contains(Constants.PROXY_PASSWORD)) {
        throw e;
      }
    }
  }
}
