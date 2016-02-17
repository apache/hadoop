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

import java.io.File;
import java.net.URI;
import java.io.IOException;

import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import org.junit.rules.TemporaryFolder;

public class TestS3AConfiguration {
  private static final String EXAMPLE_ID = "AKASOMEACCESSKEY";
  private static final String EXAMPLE_KEY =
      "RGV0cm9pdCBSZ/WQgY2xl/YW5lZCB1cAEXAMPLE";

  private Configuration conf;
  private S3AFileSystem fs;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AConfiguration.class);

  private static final String TEST_ENDPOINT = "test.fs.s3a.endpoint";

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

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
  public void testEndpoint() throws Exception {
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
  public void testProxyConnection() throws Exception {
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
  public void testProxyPortWithoutHost() throws Exception {
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
  public void testAutomaticProxyPortSelection() throws Exception {
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
  public void testUsernameInconsistentWithPassword() throws Exception {
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

  @Test
  public void testCredsFromCredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    S3AFileSystem s3afs = new S3AFileSystem();
    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    S3AFileSystem.AWSAccessKeys creds =
        s3afs.getAWSAccessKeys(new URI("s3a://foobar"), conf);
    assertEquals("AccessKey incorrect.", EXAMPLE_ID, creds.getAccessKey());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getAccessSecret());
  }

  void provisionAccessKeys(final Configuration conf) throws Exception {
    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(Constants.ACCESS_KEY,
        EXAMPLE_ID.toCharArray());
    provider.createCredentialEntry(Constants.SECRET_KEY,
        EXAMPLE_KEY.toCharArray());
    provider.flush();
  }

  @Test
  public void testCredsFromUserInfo() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    S3AFileSystem s3afs = new S3AFileSystem();
    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123:456@foobar");
    S3AFileSystem.AWSAccessKeys creds =
        s3afs.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getAccessKey());
    assertEquals("SecretKey incorrect.", "456", creds.getAccessSecret());
  }

  @Test
  public void testIDFromUserInfoSecretFromCredentialProvider()
      throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    S3AFileSystem s3afs = new S3AFileSystem();
    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123@foobar");
    S3AFileSystem.AWSAccessKeys creds =
        s3afs.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getAccessKey());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getAccessSecret());
  }

  @Test
  public void testSecretFromCredentialProviderIDFromConfig() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(Constants.SECRET_KEY,
        EXAMPLE_KEY.toCharArray());
    provider.flush();

    S3AFileSystem s3afs = new S3AFileSystem();
    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID);
    S3AFileSystem.AWSAccessKeys creds =
        s3afs.getAWSAccessKeys(new URI("s3a://foobar"), conf);
    assertEquals("AccessKey incorrect.", EXAMPLE_ID, creds.getAccessKey());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getAccessSecret());
  }

  @Test
  public void testIDFromCredentialProviderSecretFromConfig() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(Constants.ACCESS_KEY,
        EXAMPLE_ID.toCharArray());
    provider.flush();

    S3AFileSystem s3afs = new S3AFileSystem();
    conf.set(Constants.SECRET_KEY, EXAMPLE_KEY);
    S3AFileSystem.AWSAccessKeys creds =
        s3afs.getAWSAccessKeys(new URI("s3a://foobar"), conf);
    assertEquals("AccessKey incorrect.", EXAMPLE_ID, creds.getAccessKey());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getAccessSecret());
  }
}
