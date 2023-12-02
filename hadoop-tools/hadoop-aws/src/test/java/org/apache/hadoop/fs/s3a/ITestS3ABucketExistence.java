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

package org.apache.hadoop.fs.s3a;

import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.impl.InternalConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESSPOINT_REQUIRED;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A;
import static org.apache.hadoop.fs.s3a.Constants.PATH_STYLE_ACCESS;
import static org.apache.hadoop.fs.s3a.Constants.S3A_BUCKET_PROBE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Class to test bucket existence APIs.
 */
public class ITestS3ABucketExistence extends AbstractS3ATestBase {

  private FileSystem fs;

  private final String randomBucket =
          "random-bucket-" + UUID.randomUUID();

  private final URI uri = URI.create(FS_S3A + "://" + randomBucket + "/");

  @SuppressWarnings("deprecation")
  @Test
  public void testNoBucketProbing() throws Exception {
    describe("Disable init-time probes and expect FS operations to fail");
    Configuration conf = createConfigurationWithProbe(0);

    fs = FileSystem.get(uri, conf);

    Path root = new Path(uri);

    //See HADOOP-17323.
    assertTrue("root path should always exist", fs.exists(root));
    assertTrue("getFileStatus on root should always return a directory",
            fs.getFileStatus(root).isDirectory());

    try {
      expectUnknownStore(
          () -> fs.listStatus(root));
    } catch (AccessDeniedException e) {
      // this is a sign that there's tests with a third-party bucket and
      // interacting with aws is not going to authenticate
      skip("no aws credentials");
    }

    Path src = new Path(root, "testfile");
    Path dest = new Path(root, "dst");
    expectUnknownStore(
        () -> fs.getFileStatus(src));

    // the exception must not be caught and marked down to an FNFE
    expectUnknownStore(() -> fs.exists(src));
    // now that isFile() only does a HEAD, it will get a 404 without
    // the no-such-bucket error.
    assertFalse("isFile(" + src + ")"
            + " was expected to complete by returning false",
        fs.isFile(src));
    expectUnknownStore(() -> fs.isDirectory(src));
    expectUnknownStore(() -> fs.mkdirs(src));
    expectUnknownStore(() -> fs.delete(src));
    expectUnknownStore(() -> fs.rename(src, dest));

    byte[] data = dataset(1024, 'a', 'z');
    expectUnknownStore(
        () -> writeDataset(fs, src, data, data.length, 1024 * 1024, true));
  }

  /**
   * Expect an operation to raise an UnknownStoreException.
   * @param eval closure
   * @param <T> return type of closure
   * @throws Exception anything else raised.
   */
  public static <T> void expectUnknownStore(
      Callable<T> eval)
      throws Exception {
    intercept(UnknownStoreException.class, eval);
  }

  /**
   * Expect an operation to raise an UnknownStoreException.
   * @param eval closure
   * @throws Exception anything else raised.
   */
  public static void expectUnknownStore(
      LambdaTestUtils.VoidCallable eval)
      throws Exception {
    intercept(UnknownStoreException.class, eval);
  }

  /**
   * Create a new configuration with the given bucket probe;
   * we also disable FS caching.
   * @param probe value to use as the bucket probe.
   * @return a configuration.
   */
  private Configuration createConfigurationWithProbe(final int probe) {
    Configuration conf = new Configuration(getFileSystem().getConf());
    S3ATestUtils.disableFilesystemCaching(conf);
    removeBaseAndBucketOverrides(conf,
        S3A_BUCKET_PROBE,
        ENDPOINT,
        AWS_REGION,
        PATH_STYLE_ACCESS);
    conf.setInt(S3A_BUCKET_PROBE, probe);
    conf.set(AWS_REGION, EU_WEST_1);
    return conf;
  }

  @Test
  public void testBucketProbing() throws Exception {
    describe("Test the V1 bucket probe");
    Configuration configuration = createConfigurationWithProbe(1);
    expectUnknownStore(
        () -> FileSystem.get(uri, configuration));
  }

  @Test
  public void testBucketProbing2() throws Exception {
    describe("Test the bucket probe with probe value set to 2");
    Configuration configuration = createConfigurationWithProbe(2);

    expectUnknownStore(
        () -> FileSystem.get(uri, configuration));
  }

  @Test
  public void testBucketProbing3() throws Exception {
    describe("Test the bucket probe with probe value set to 3");
    Configuration configuration = createConfigurationWithProbe(3);
    fs = FileSystem.get(uri, configuration);
    Path root = new Path(uri);

    assertTrue("root path should always exist", fs.exists(root));
    assertTrue("getFileStatus on root should always return a directory",
        fs.getFileStatus(root).isDirectory());
  }

  @Test
  public void testBucketProbingParameterValidation() throws Exception {
    describe("Test bucket probe parameter %s validation", S3A_BUCKET_PROBE);
    Configuration configuration = createConfigurationWithProbe(-1);
    intercept(IllegalArgumentException.class,
            "Value of " + S3A_BUCKET_PROBE + " should be >= 0",
            "Should throw IllegalArgumentException",
        () -> FileSystem.get(uri, configuration));
  }

  @Test
  public void testAccessPointProbing2() throws Exception {
    describe("Test bucket probing using probe value 2, and an AccessPoint ARN");
    Configuration configuration = createArnConfiguration();
    String accessPointArn = "arn:aws:s3:eu-west-1:123456789012:accesspoint/" + randomBucket;
    configuration.set(String.format(InternalConstants.ARN_BUCKET_OPTION, randomBucket),
        accessPointArn);

    expectUnknownStore(
        () -> FileSystem.get(uri, configuration));
  }

  @Test
  public void testAccessPointRequired() throws Exception {
    describe("Test bucket probing with 'fs.s3a.accesspoint.required' property.");
    Configuration configuration = createArnConfiguration();
    configuration.set(AWS_S3_ACCESSPOINT_REQUIRED, "true");
    intercept(PathIOException.class,
        InternalConstants.AP_REQUIRED_EXCEPTION,
        "Should throw IOException if Access Points are required but not configured.",
        () -> FileSystem.get(uri, configuration));

    String accessPointArn = "arn:aws:s3:eu-west-1:123456789012:accesspoint/" + randomBucket;
    configuration.set(String.format(InternalConstants.ARN_BUCKET_OPTION, randomBucket),
        accessPointArn);
    expectUnknownStore(
        () -> FileSystem.get(uri, configuration));
  }

  /**
   * Create a configuration which has bucket probe 2 and the endpoint.region
   * option set to "eu-west-1" to match that of the ARNs generated.
   * @return a configuration for tests which are expected to fail in specific ways.
   */
  private Configuration createArnConfiguration() {
    Configuration configuration = createConfigurationWithProbe(2);
    configuration.set(AWS_REGION, EU_WEST_1);
    return configuration;
  }

  @Override
  protected Configuration getConfiguration() {
    Configuration configuration = super.getConfiguration();
    S3ATestUtils.disableFilesystemCaching(configuration);
    return configuration;
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(getLogger(), fs);
    super.teardown();
  }
}
