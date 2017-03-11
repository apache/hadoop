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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.AccessDeniedException;

import static org.apache.hadoop.fs.s3a.S3ATestConstants.TEST_FS_S3A_NAME;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeS3GuardState;

/**
 * Tests that credentials can go into the URL. This includes a valid
 * set, and a check that an invalid set do at least get stripped out
 * of the final URI
 */
public class ITestS3ACredentialsInURL extends Assert {
  private S3AFileSystem fs;
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ACredentialsInURL.class);
  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @After
  public void teardown() {
    IOUtils.closeStream(fs);
  }

  /**
   * Test instantiation.
   * @throws Throwable
   */
  @Test
  public void testInstantiateFromURL() throws Throwable {

    Configuration conf = new Configuration();

    // Skip in the case of S3Guard with DynamoDB because it cannot get
    // credentials for its own use if they're only in S3 URLs
    assumeS3GuardState(false, conf);

    String accessKey = conf.get(Constants.ACCESS_KEY);
    String secretKey = conf.get(Constants.SECRET_KEY);
    String fsname = conf.getTrimmed(TEST_FS_S3A_NAME, "");
    Assume.assumeNotNull(fsname, accessKey, secretKey);
    URI original = new URI(fsname);
    URI secretsURI = createUriWithEmbeddedSecrets(original,
        accessKey, secretKey);
    if (secretKey.contains("/")) {
      assertTrue("test URI encodes the / symbol", secretsURI.toString().
          contains("%252F"));
    }
    if (secretKey.contains("+")) {
      assertTrue("test URI encodes the + symbol", secretsURI.toString().
          contains("%252B"));
    }
    assertFalse("Does not contain secrets", original.equals(secretsURI));

    conf.set(TEST_FS_S3A_NAME, secretsURI.toString());
    conf.unset(Constants.ACCESS_KEY);
    conf.unset(Constants.SECRET_KEY);
    fs = S3ATestUtils.createTestFileSystem(conf);

    String fsURI = fs.getUri().toString();
    assertFalse("FS URI contains a @ symbol", fsURI.contains("@"));
    assertFalse("FS URI contains a % symbol", fsURI.contains("%"));
    if (!original.toString().startsWith(fsURI)) {
      fail("Filesystem URI does not match original");
    }
    validate("original path", new Path(original));
    validate("bare path", new Path("/"));
    validate("secrets path", new Path(secretsURI));
  }

  private void validate(String text, Path path) throws IOException {
    try {
      fs.canonicalizeUri(path.toUri());
      fs.checkPath(path);
      assertTrue(text + " Not a directory",
          fs.getFileStatus(new Path("/")).isDirectory());
      fs.globStatus(path);
    } catch (AssertionError e) {
      throw e;
    } catch (Exception e) {
      LOG.debug("{} failure: {}", text, e, e);
      fail(text + " Test failed");
    }
  }

  /**
   * Set up some invalid credentials, verify login is rejected.
   * @throws Throwable
   */
  @Test
  public void testInvalidCredentialsFail() throws Throwable {
    Configuration conf = new Configuration();
    String fsname = conf.getTrimmed(TEST_FS_S3A_NAME, "");
    Assume.assumeNotNull(fsname);
    assumeS3GuardState(false, conf);
    URI original = new URI(fsname);
    URI testURI = createUriWithEmbeddedSecrets(original, "user", "//");

    conf.set(TEST_FS_S3A_NAME, testURI.toString());
    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      FileStatus status = fs.getFileStatus(new Path("/"));
      fail("Expected an AccessDeniedException, got " + status);
    } catch (AccessDeniedException e) {
      // expected
    }

  }

  private URI createUriWithEmbeddedSecrets(URI original,
      String accessKey,
      String secretKey) throws UnsupportedEncodingException {
    String encodedSecretKey = URLEncoder.encode(secretKey, "UTF-8");
    String formattedString = String.format("%s://%s:%s@%s/%s/",
        original.getScheme(),
        accessKey,
        encodedSecretKey,
        original.getHost(),
        original.getPath());
    URI testURI;
    try {
      testURI = new Path(formattedString).toUri();
    } catch (IllegalArgumentException e) {
      // inner cause is stripped to keep any secrets out of stack traces
      throw new IllegalArgumentException("Could not encode Path");
    }
    return testURI;
  }
}
