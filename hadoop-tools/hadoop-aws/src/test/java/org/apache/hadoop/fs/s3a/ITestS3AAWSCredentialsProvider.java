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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_BINDING;
import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.CONSTRUCTOR_EXCEPTION;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.getExternalData;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.*;

/**
 * Integration tests for {@link Constants#AWS_CREDENTIALS_PROVIDER} logic
 * through the S3A Filesystem instantiation process.
 */
public class ITestS3AAWSCredentialsProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AAWSCredentialsProvider.class);

  @Rule
  public Timeout testTimeout = new Timeout(60_1000, TimeUnit.MILLISECONDS);

  /**
   * Expecting a wrapped ClassNotFoundException.
   */
  @Test
  public void testProviderClassNotFound() throws Exception {
    Configuration conf = createConf("no.such.class");
    final InstantiationIOException e =
        intercept(InstantiationIOException.class, "java.lang.ClassNotFoundException", () ->
            createFailingFS(conf));
    if (InstantiationIOException.Kind.InstantiationFailure != e.getKind()) {
      throw e;
    }
    if (!(e.getCause() instanceof ClassNotFoundException)) {
      LOG.error("Unexpected nested cause: {} in {}", e.getCause(), e, e);
      throw e;
    }
  }

  /**
   * A bad CredentialsProvider which has no suitable constructor.
   *
   * This class does not provide a public constructor accepting Configuration,
   * or a public factory method named create() that accepts no arguments,
   * or a public default constructor.
   */
  public static class BadCredentialsProviderConstructor
      implements AwsCredentialsProvider {

    @SuppressWarnings("unused")
    public BadCredentialsProviderConstructor(String fsUri, Configuration conf) {
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return AwsBasicCredentials.create("dummy_key", "dummy_secret");
    }

  }

  @Test
  public void testBadCredentialsConstructor() throws Exception {
    Configuration conf = createConf(BadCredentialsProviderConstructor.class);
    final InstantiationIOException ex =
        intercept(InstantiationIOException.class, CONSTRUCTOR_EXCEPTION, () ->
            createFailingFS(conf));
    if (InstantiationIOException.Kind.UnsupportedConstructor != ex.getKind()) {
      throw ex;
    }
  }

  /**
   * Test aws credentials provider remapping with key that maps to
   * BadCredentialsProviderConstructor.
   */
  @Test
  public void testBadCredentialsConstructorWithRemap() throws Exception {
    Configuration conf = createConf("aws.test.map1");
    conf.set(AWS_CREDENTIALS_PROVIDER_MAPPING,
        "aws.test.map1=" + BadCredentialsProviderConstructor.class.getName());
    final InstantiationIOException ex =
        intercept(InstantiationIOException.class, CONSTRUCTOR_EXCEPTION, () ->
            createFailingFS(conf));
    if (InstantiationIOException.Kind.UnsupportedConstructor != ex.getKind()) {
      throw ex;
    }
  }

  /**
   * Create a configuration bonded to the given provider classname.
   * @param provider provider to bond to
   * @return a configuration
   */
  protected Configuration createConf(String provider) {
    Configuration conf = new Configuration();
    removeBaseAndBucketOverrides(conf,
        DELEGATION_TOKEN_BINDING,
        AWS_CREDENTIALS_PROVIDER);
    conf.set(AWS_CREDENTIALS_PROVIDER, provider);
    conf.set(DELEGATION_TOKEN_BINDING, "");
    return conf;
  }

  /**
   * Create a configuration bonded to the given provider class.
   * @param provider provider to bond to
   * @return a configuration
   */
  protected Configuration createConf(Class provider) {
    return createConf(provider.getName());
  }

  /**
   * Create a filesystem, expect it to fail by raising an IOException.
   * Raises an assertion exception if in fact the FS does get instantiated.
   * The FS is always deleted.
   * @param conf configuration
   * @throws IOException an expected exception.
   */
  private void createFailingFS(Configuration conf) throws IOException {
    try(S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      fs.listStatus(new Path("/"));
      fail("Expected exception - got " + fs);
    }
  }

  /**
   * Returns an invalid set of credentials.
   */
  public static class BadCredentialsProvider implements AwsCredentialsProvider {

    @SuppressWarnings("unused")
    public BadCredentialsProvider(Configuration conf) {
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return AwsBasicCredentials.create("bad_key", "bad_secret");
    }

  }

  @Test
  public void testBadCredentials() throws Exception {
    Configuration conf = createConf(BadCredentialsProvider.class);
    intercept(AccessDeniedException.class, "", () ->
        createFailingFS(conf));
  }

  /**
   * Test aws credentials provider remapping with key that maps to
   * BadCredentialsProvider.
   */
  @Test
  public void testBadCredentialsWithRemap() throws Exception {
    Configuration conf = createConf("aws.test.map.key");
    conf.set(AWS_CREDENTIALS_PROVIDER_MAPPING,
        "aws.test.map.key=" + BadCredentialsProvider.class.getName());
    intercept(AccessDeniedException.class,
        "",
        () -> createFailingFS(conf));
  }

  /**
   * Test using the anonymous credential provider with the public csv
   * test file; if the test file path is unset then it will be skipped.
   */
  @Test
  public void testAnonymousProvider() throws Exception {
    Configuration conf = createConf(AnonymousAWSCredentialsProvider.class);
    Path testFile = getExternalData(conf);
    try (FileSystem fs = FileSystem.newInstance(testFile.toUri(), conf)) {
      Assertions.assertThat(fs)
          .describedAs("Filesystem")
          .isNotNull();
      FileStatus stat = fs.getFileStatus(testFile);
      assertEquals(
          "The qualified path returned by getFileStatus should be same as the original file",
          testFile, stat.getPath());
    }
  }

  /**
   * Create credentials via the create() method.
   * They are invalid credentials, so IO will fail as access denied.
   */
  @Test
  public void testCredentialsWithCreateMethod() throws Exception {
    Configuration conf = createConf(CredentialsProviderWithCreateMethod.class);
    intercept(AccessDeniedException.class, "", () ->
        createFailingFS(conf));
  }

  /**
   * Credentials via the create() method.
   */
  public static final class CredentialsProviderWithCreateMethod implements AwsCredentialsProvider {

    public static AwsCredentialsProvider create() {
      LOG.info("creating CredentialsProviderWithCreateMethod");
      return new CredentialsProviderWithCreateMethod();
    }

    /** Private: cannot be created directly. */
    private CredentialsProviderWithCreateMethod() {
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return AwsBasicCredentials.create("bad_key", "bad_secret");
    }

  }

}
