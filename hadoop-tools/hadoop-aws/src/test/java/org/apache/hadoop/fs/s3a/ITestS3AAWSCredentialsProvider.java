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

import java.io.IOException;
import java.nio.file.AccessDeniedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.junit.Assert.*;

/**
 * Integration tests for {@link Constants#AWS_CREDENTIALS_PROVIDER} logic.
 */
public class ITestS3AAWSCredentialsProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AAWSCredentialsProvider.class);

  @Rule
  public Timeout testTimeout = new Timeout(1 * 60 * 1000);

  @Test
  public void testBadConfiguration() throws IOException {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER, "no.such.class");
    try {
      createFailingFS(conf);
    } catch (IOException e) {
      if (!(e.getCause() instanceof ClassNotFoundException)) {
        LOG.error("Unexpected nested cause: {} in {}", e.getCause(), e, e);
        throw e;
      }
    }
  }

  /**
   * A bad CredentialsProvider which has no suitable constructor.
   *
   * This class does not provide a public constructor accepting Configuration,
   * or a public factory method named getInstance that accepts no arguments,
   * or a public default constructor.
   */
  static class BadCredentialsProviderConstructor
      implements AWSCredentialsProvider {

    @SuppressWarnings("unused")
    public BadCredentialsProviderConstructor(String fsUri, Configuration conf) {
    }

    @Override
    public AWSCredentials getCredentials() {
      return new BasicAWSCredentials("dummy_key", "dummy_secret");
    }

    @Override
    public void refresh() {
    }
  }

  @Test
  public void testBadCredentialsConstructor() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER,
        BadCredentialsProviderConstructor.class.getName());
    try {
      createFailingFS(conf);
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(CONSTRUCTOR_EXCEPTION, e);
    }
  }

  /**
   * Create a filesystem, expect it to fail by raising an IOException.
   * Raises an assertion exception if in fact the FS does get instantiated.
   * @param conf configuration
   * @throws IOException an expected exception.
   */
  private void createFailingFS(Configuration conf) throws IOException {
    S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf);
    fs.listStatus(new Path("/"));
    fail("Expected exception - got " + fs);
  }

  static class BadCredentialsProvider implements AWSCredentialsProvider {

    @SuppressWarnings("unused")
    public BadCredentialsProvider(Configuration conf) {
    }

    @Override
    public AWSCredentials getCredentials() {
      return new BasicAWSCredentials("bad_key", "bad_secret");
    }

    @Override
    public void refresh() {
    }
  }

  @Test
  public void testBadCredentials() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER, BadCredentialsProvider.class.getName());
    try {
      createFailingFS(conf);
    } catch (AccessDeniedException e) {
      // expected
    } catch (AWSServiceIOException e) {
      GenericTestUtils.assertExceptionContains(
          "UnrecognizedClientException", e);
      // expected
    }
  }

  @Test
  public void testAnonymousProvider() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER,
        AnonymousAWSCredentialsProvider.class.getName());
    Path testFile = new Path(
        conf.getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE));
    FileSystem fs = FileSystem.newInstance(testFile.toUri(), conf);
    assertNotNull(fs);
    assertTrue(fs instanceof S3AFileSystem);
    FileStatus stat = fs.getFileStatus(testFile);
    assertNotNull(stat);
    assertEquals(testFile, stat.getPath());
  }

}
