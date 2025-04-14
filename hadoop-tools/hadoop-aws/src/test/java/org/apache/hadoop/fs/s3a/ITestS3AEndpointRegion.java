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

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.awscore.AwsExecutionAttribute;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;
import org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.ALLOW_REQUESTER_PAYS;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CROSS_REGION_ACCESS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.FIPS_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.PATH_STYLE_ACCESS;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.ERROR_ENDPOINT_WITH_FIPS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeStoreAwsHosted;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.DEFAULT_REQUESTER_PAYS_BUCKET_NAME;
import static org.apache.hadoop.io.IOUtils.closeStream;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test to check correctness of S3A endpoint regions in
 * {@link DefaultS3ClientFactory}.
 */
public class ITestS3AEndpointRegion extends AbstractS3ATestBase {

  private static final String AWS_ENDPOINT_TEST = "test-endpoint";

  private static final String US_EAST_1 = "us-east-1";

  private static final String US_EAST_2 = "us-east-2";

  private static final String US_WEST_2 = "us-west-2";

  private static final String SA_EAST_1 = "sa-east-1";

  private static final String EU_WEST_2 = "eu-west-2";

  private static final String CN_NORTHWEST_1 = "cn-northwest-1";

  private static final String US_GOV_EAST_1 = "us-gov-east-1";

  private static final String US_REGION_PREFIX = "us-";

  private static final String CA_REGION_PREFIX = "ca-";

  private static final String US_DUAL_STACK_PREFIX = "dualstack.us-";

  /**
   * If anyone were ever to create a bucket with this UUID pair it would break the tests.
   */
  public static final String UNKNOWN_BUCKET = "23FA76D4-5F17-48B8-9D7D-9050269D0E40"
      + "-8281BAF2-DBCF-47AA-8A27-F2FA3589656A";

  private static final String EU_WEST_2_ENDPOINT = "s3.eu-west-2.amazonaws.com";

  private static final String CN_ENDPOINT = "s3.cn-northwest-1.amazonaws.com.cn";

  private static final String GOV_ENDPOINT = "s3-fips.us-gov-east-1.amazonaws.com";

  private static final String VPC_ENDPOINT = "vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com";

  private static final String CN_VPC_ENDPOINT = "vpce-1a2b3c4d-5e6f.s3.cn-northwest-1.vpce.amazonaws.com.cn";

  public static final String EXCEPTION_THROWN_BY_INTERCEPTOR = "Exception thrown by interceptor";

  /**
   * Text to include in assertions.
   */
  private static final AtomicReference<String> EXPECTED_MESSAGE = new AtomicReference<>();
  /**
   * New FS instance which will be closed in teardown.
   */
  private S3AFileSystem newFS;

  @Override
  public void teardown() throws Exception {
    closeStream(newFS);
    super.teardown();
  }

  @Test
  public void testWithoutRegionConfig() throws IOException {
    describe("Verify that region lookup takes place");

    Configuration conf = getConfiguration();
    removeBaseAndBucketOverrides(conf, AWS_REGION, PATH_STYLE_ACCESS);
    conf.setBoolean(PATH_STYLE_ACCESS, false);

    newFS = new S3AFileSystem();

    try {
      newFS.initialize(getFileSystem().getUri(), conf);
      newFS.getS3AInternals().getBucketMetadata();
    } catch (UnknownHostException | UnknownStoreException | AccessDeniedException allowed) {
      // these are all valid failure modes from different test environments.
    }
  }

  @Test
  public void testUnknownBucket() throws Exception {
    describe("Verify unknown bucket probe failure");
    Configuration conf = getConfiguration();
    removeBaseAndBucketOverrides(UNKNOWN_BUCKET, conf, AWS_REGION, PATH_STYLE_ACCESS);

    newFS = new S3AFileSystem();

    try {
      newFS.initialize(new URI("s3a://" + UNKNOWN_BUCKET), conf);
      newFS.getS3AInternals().getBucketMetadata();
      // expect a failure by here
      fail("Expected failure, got " + newFS);
    } catch (UnknownHostException | UnknownStoreException expected) {
      // this is good.
    }
  }

  @Test
  public void testEndpointOverride() throws Throwable {
    describe("Create a client with a configured endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, AWS_ENDPOINT_TEST, null, US_EAST_2, false);

    expectInterceptorException(client);
  }

  @Test
  public void testCentralEndpoint() throws Throwable {
    describe("Create a client with the central endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, CENTRAL_ENDPOINT, null, US_EAST_2, false);

    expectInterceptorException(client);

    client = createS3Client(conf, CENTRAL_ENDPOINT, null,
        US_EAST_2, true);

    expectInterceptorException(client);
  }

  @Test
  public void testCentralEndpointWithRegion() throws Throwable {
    describe("Create a client with the central endpoint but also specify region");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, CENTRAL_ENDPOINT, US_WEST_2,
        US_WEST_2, false);

    expectInterceptorException(client);

    client = createS3Client(conf, CENTRAL_ENDPOINT, US_WEST_2,
        US_WEST_2, true);

    expectInterceptorException(client);

    client = createS3Client(conf, CENTRAL_ENDPOINT, US_EAST_1,
        US_EAST_1, false);

    expectInterceptorException(client);

    client = createS3Client(conf, CENTRAL_ENDPOINT, US_EAST_1,
        US_EAST_1, true);

    expectInterceptorException(client);

  }

  @Test
  public void testWithRegionConfig() throws Throwable {
    describe("Create a client with a configured region");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, null, EU_WEST_2, EU_WEST_2, false);

    expectInterceptorException(client);
  }

  @Test
  public void testWithFips() throws Throwable {
    describe("Create a client with fips enabled");

    S3Client client = createS3Client(getConfiguration(),
        null, EU_WEST_2, EU_WEST_2, true);
    expectInterceptorException(client);
  }

  /**
   * Attempting to create a client with fips enabled and an endpoint specified
   * fails during client construction.
   */
  @Test
  public void testWithFipsAndEndpoint() throws Throwable {
    describe("Create a client with fips and an endpoint");

    intercept(IllegalArgumentException.class, ERROR_ENDPOINT_WITH_FIPS, () ->
        createS3Client(getConfiguration(), US_WEST_2, null, US_EAST_1, true));
  }

  @Test
  public void testEUWest2Endpoint() throws Throwable {
    describe("Create a client with the eu west 2 endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, EU_WEST_2_ENDPOINT, null, EU_WEST_2, false);

    expectInterceptorException(client);
  }

  @Test
  public void testWithRegionAndEndpointConfig() throws Throwable {
    describe("Test that when both region and endpoint are configured, region takes precedence");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, EU_WEST_2_ENDPOINT, US_WEST_2, US_WEST_2, false);

    expectInterceptorException(client);
  }

  @Test
  public void testWithChinaEndpoint() throws Throwable {
    describe("Test with a china endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, CN_ENDPOINT, null, CN_NORTHWEST_1, false);

    expectInterceptorException(client);
  }

  /**
   * Expect an exception to be thrown by the interceptor with the message
   * {@link #EXCEPTION_THROWN_BY_INTERCEPTOR}.
   * @param client client to issue a head request against.
   * @return the expected exception.
   * @throws Exception any other exception.
   */
  private AwsServiceException expectInterceptorException(final S3Client client)
      throws Exception {

    return intercept(AwsServiceException.class, EXCEPTION_THROWN_BY_INTERCEPTOR,
        () -> head(client));
  }

  /**
   * Issue a head request against the bucket.
   * @param client client to use
   * @return the response.
   */
  private HeadBucketResponse head(final S3Client client) {
    return client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build());
  }

  @Test
  public void testWithGovCloudEndpoint() throws Throwable {
    describe("Test with a gov cloud endpoint; enable fips");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, GOV_ENDPOINT, null, US_GOV_EAST_1, false);

    expectInterceptorException(client);
  }

  @Test
  public void testWithVPCE() throws Throwable {
    describe("Test with vpc endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, VPC_ENDPOINT, null, US_WEST_2, false);

    expectInterceptorException(client);
  }

  @Test
  public void testWithChinaVPCE() throws Throwable {
    describe("Test with china vpc endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, CN_VPC_ENDPOINT, null, CN_NORTHWEST_1, false);

    expectInterceptorException(client);
  }

  @Test
  public void testCentralEndpointAndDifferentRegionThanBucket() throws Throwable {
    describe("Access public bucket using central endpoint and region "
        + "different than that of the public bucket");
    final Configuration conf = getConfiguration();
    final Configuration newConf = new Configuration(conf);

    removeBaseAndBucketOverrides(
        newConf,
        ENDPOINT,
        AWS_REGION,
        ALLOW_REQUESTER_PAYS,
        KEY_REQUESTER_PAYS_FILE,
        FIPS_ENDPOINT);

    removeBaseAndBucketOverrides(
        DEFAULT_REQUESTER_PAYS_BUCKET_NAME,
        newConf,
        ENDPOINT,
        AWS_REGION,
        ALLOW_REQUESTER_PAYS,
        KEY_REQUESTER_PAYS_FILE,
        FIPS_ENDPOINT);

    newConf.set(ENDPOINT, CENTRAL_ENDPOINT);
    newConf.set(AWS_REGION, EU_WEST_1);
    newConf.setBoolean(ALLOW_REQUESTER_PAYS, true);

    assertRequesterPaysFileExistence(newConf);
  }

  @Test
  public void testWithOutCrossRegionAccess() throws Exception {
    describe("Verify cross region access fails when disabled");
    // skip the test if the region is sa-east-1
    skipCrossRegionTest();
    final Configuration newConf = new Configuration(getConfiguration());
    removeBaseAndBucketOverrides(newConf,
        ENDPOINT,
        AWS_S3_CROSS_REGION_ACCESS_ENABLED,
        AWS_REGION);
    // disable cross region access
    newConf.setBoolean(AWS_S3_CROSS_REGION_ACCESS_ENABLED, false);
    newConf.set(AWS_REGION, SA_EAST_1);
    try (S3AFileSystem fs = new S3AFileSystem()) {
      fs.initialize(getFileSystem().getUri(), newConf);
      intercept(AWSRedirectException.class,
          "does not match the AWS region containing the bucket",
          () -> fs.exists(getFileSystem().getWorkingDirectory()));
    }
  }

  @Test
  public void testWithCrossRegionAccess() throws Exception {
    describe("Verify cross region access succeed when enabled");
    // skip the test if the region is sa-east-1
    skipCrossRegionTest();
    final Configuration newConf = new Configuration(getConfiguration());
    removeBaseAndBucketOverrides(newConf,
        ENDPOINT,
        AWS_S3_CROSS_REGION_ACCESS_ENABLED,
        AWS_REGION);
    // enable cross region access
    newConf.setBoolean(AWS_S3_CROSS_REGION_ACCESS_ENABLED, true);
    newConf.set(AWS_REGION, SA_EAST_1);
    try (S3AFileSystem fs = new S3AFileSystem()) {
      fs.initialize(getFileSystem().getUri(), newConf);
      fs.exists(getFileSystem().getWorkingDirectory());
    }
  }

  @Test
  public void testCentralEndpointAndSameRegionAsBucket() throws Throwable {
    describe("Access public bucket using central endpoint and region "
        + "same as that of the public bucket");
    final Configuration conf = getConfiguration();
    final Configuration newConf = new Configuration(conf);

    removeBaseAndBucketOverrides(
        newConf,
        ENDPOINT,
        AWS_REGION,
        ALLOW_REQUESTER_PAYS,
        KEY_REQUESTER_PAYS_FILE,
        FIPS_ENDPOINT);

    removeBaseAndBucketOverrides(
        DEFAULT_REQUESTER_PAYS_BUCKET_NAME,
        newConf,
        ENDPOINT,
        AWS_REGION,
        ALLOW_REQUESTER_PAYS,
        KEY_REQUESTER_PAYS_FILE,
        FIPS_ENDPOINT);

    newConf.set(ENDPOINT, CENTRAL_ENDPOINT);
    newConf.set(AWS_REGION, US_WEST_2);
    newConf.setBoolean(ALLOW_REQUESTER_PAYS, true);

    assertRequesterPaysFileExistence(newConf);
  }

  @Test
  public void testCentralEndpointAndFipsForPublicBucket() throws Throwable {
    describe("Access public bucket using central endpoint and region "
        + "same as that of the public bucket with fips enabled");
    final Configuration conf = getConfiguration();
    final Configuration newConf = new Configuration(conf);

    removeBaseAndBucketOverrides(
        newConf,
        ENDPOINT,
        AWS_REGION,
        ALLOW_REQUESTER_PAYS,
        KEY_REQUESTER_PAYS_FILE,
        FIPS_ENDPOINT);

    removeBaseAndBucketOverrides(
        DEFAULT_REQUESTER_PAYS_BUCKET_NAME,
        newConf,
        ENDPOINT,
        AWS_REGION,
        ALLOW_REQUESTER_PAYS,
        KEY_REQUESTER_PAYS_FILE,
        FIPS_ENDPOINT);

    newConf.set(ENDPOINT, CENTRAL_ENDPOINT);
    newConf.set(AWS_REGION, US_WEST_2);
    newConf.setBoolean(ALLOW_REQUESTER_PAYS, true);
    newConf.setBoolean(FIPS_ENDPOINT, true);

    assertRequesterPaysFileExistence(newConf);
  }

  /**
   * Assert that the file exists on the requester pays public bucket.
   *
   * @param conf the configuration object.
   * @throws IOException if file system operations encounter errors.
   */
  private void assertRequesterPaysFileExistence(Configuration conf)
      throws IOException {
    Path filePath = new Path(PublicDatasetTestUtils
        .getRequesterPaysObject(conf));
    newFS = (S3AFileSystem) filePath.getFileSystem(conf);

    Assertions
        .assertThat(newFS.exists(filePath))
        .describedAs("Existence of path: " + filePath)
        .isTrue();
  }

  @Test
  public void testCentralEndpointAndNullRegionWithCRUD() throws Throwable {
    describe("Access the test bucket using central endpoint and"
        + " null region, perform file system CRUD operations");
    final Configuration conf = getConfiguration();
    assumeStoreAwsHosted(getFileSystem());

    final Configuration newConf = new Configuration(conf);

    removeBaseAndBucketOverrides(
        newConf,
        ENDPOINT,
        AWS_REGION,
        FIPS_ENDPOINT,
        S3_ENCRYPTION_ALGORITHM);

    newConf.set(ENDPOINT, CENTRAL_ENDPOINT);

    newFS = new S3AFileSystem();
    newFS.initialize(getFileSystem().getUri(), newConf);

    assertOpsUsingNewFs();
  }

  @Test
  public void testCentralEndpointAndNullRegionFipsWithCRUD() throws Throwable {
    describe("Access the test bucket using central endpoint and"
        + " null region and fips enabled, perform file system CRUD operations");
    assumeStoreAwsHosted(getFileSystem());

    final String bucketLocation = getFileSystem().getBucketLocation();
    assume("FIPS can be enabled to access buckets from US or Canada endpoints only",
        bucketLocation.startsWith(US_REGION_PREFIX)
            || bucketLocation.startsWith(CA_REGION_PREFIX)
            || bucketLocation.startsWith(US_DUAL_STACK_PREFIX));

    final Configuration conf = getConfiguration();
    final Configuration newConf = new Configuration(conf);

    removeBaseAndBucketOverrides(
        newConf,
        ENDPOINT,
        AWS_REGION,
        FIPS_ENDPOINT);

    newConf.set(ENDPOINT, CENTRAL_ENDPOINT);
    newConf.setBoolean(FIPS_ENDPOINT, true);

    newFS = new S3AFileSystem();
    newFS.initialize(getFileSystem().getUri(), newConf);

    assertOpsUsingNewFs();
  }

  /**
   * Skip the test if the region is null or sa-east-1.
   */
  private void skipCrossRegionTest() throws IOException {
    String region = getFileSystem().getS3AInternals().getBucketMetadata().bucketRegion();
    if (region == null || SA_EAST_1.equals(region)) {
      skip("Skipping test since region is null or it is set to sa-east-1");
    }
  }

  private void assertOpsUsingNewFs() throws IOException {
    final String file = getMethodName();
    final Path basePath = methodPath();
    final Path srcDir = new Path(basePath, "srcdir");
    newFS.mkdirs(srcDir);
    Path srcFilePath = new Path(srcDir, file);

    try (FSDataOutputStream out = newFS.create(srcFilePath)) {
      out.write(new byte[] {1, 2, 3});
    }

    Assertions
        .assertThat(newFS.exists(srcFilePath))
        .describedAs("Existence of file: " + srcFilePath)
        .isTrue();
    Assertions
        .assertThat(getFileSystem().exists(srcFilePath))
        .describedAs("Existence of file: " + srcFilePath)
        .isTrue();

    byte[] buffer = new byte[3];

    try (FSDataInputStream in = newFS.open(srcFilePath)) {
      in.readFully(buffer);
      Assertions
          .assertThat(buffer)
          .describedAs("Contents read from " + srcFilePath)
          .containsExactly(1, 2, 3);
    }

    newFS.delete(srcDir, true);

    Assertions
        .assertThat(newFS.exists(srcFilePath))
        .describedAs("Existence of file: " + srcFilePath + " using new FS")
        .isFalse();
    Assertions
        .assertThat(getFileSystem().exists(srcFilePath))
        .describedAs("Existence of file: " + srcFilePath + " using original FS")
        .isFalse();
  }

  private static final class RegionInterceptor implements ExecutionInterceptor {
    private final String endpoint;
    private final String region;
    private final boolean isFips;

    RegionInterceptor(String endpoint, String region, final boolean isFips) {
      this.endpoint = endpoint;
      this.region = region;
      this.isFips = isFips;
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context,
        ExecutionAttributes executionAttributes)  {


      // extract state from the execution attributes.
      final Boolean endpointOveridden =
          executionAttributes.getAttribute(AwsExecutionAttribute.ENDPOINT_OVERRIDDEN);
      final String clientEndpoint =
          executionAttributes.getAttribute(AwsExecutionAttribute.CLIENT_ENDPOINT).toString();
      final Boolean fipsEnabled = executionAttributes.getAttribute(
          AwsExecutionAttribute.FIPS_ENDPOINT_ENABLED);
      final String reg = executionAttributes.getAttribute(AwsExecutionAttribute.AWS_REGION).
          toString();

      String state = "SDK beforeExecution callback; "
          + "endpointOveridden=" + endpointOveridden
          + "; clientEndpoint=" + clientEndpoint
          + "; fipsEnabled=" + fipsEnabled
          + "; region=" + reg;

      if (endpoint != null && !endpoint.endsWith(CENTRAL_ENDPOINT)) {
        Assertions.assertThat(endpointOveridden)
            .describedAs("Endpoint not overridden in %s. Client Config=%s",
                state, EXPECTED_MESSAGE.get())
            .isTrue();

        Assertions.assertThat(clientEndpoint)
            .describedAs("There is an endpoint mismatch in %s. Client Config=%s",
                state, EXPECTED_MESSAGE.get())
            .isEqualTo("https://" + endpoint);
      } else {
        Assertions.assertThat(endpointOveridden)
            .describedAs("Attribute endpointOveridden is null in %s. Client Config=%s",
                state, EXPECTED_MESSAGE.get())
            .isEqualTo(false);
      }

      Assertions.assertThat(reg)
          .describedAs("Incorrect region set in %s. Client Config=%s",
              state, EXPECTED_MESSAGE.get())
          .isEqualTo(region);

      // verify the fips state matches expectation.
      Assertions.assertThat(fipsEnabled)
          .describedAs("Incorrect FIPS flag set in %s; Client Config=%s",
              state, EXPECTED_MESSAGE.get())
          .isNotNull()
          .isEqualTo(isFips);

      // We don't actually want to make a request, so exit early.
      throw AwsServiceException.builder().message(EXCEPTION_THROWN_BY_INTERCEPTOR).build();
    }
  }

  /**
   * Create an S3 client with the given conf and endpoint.
   * The region name must then match that of the expected
   * value.
   * @param conf configuration to use.
   * @param endpoint endpoint.
   * @param expectedRegion the region that should be set in the client.
   * @param isFips is this a FIPS endpoint?
   * @return the client.
   * @throws IOException IO problems
   */
  @SuppressWarnings("deprecation")
  private S3Client createS3Client(Configuration conf,
      String endpoint, String configuredRegion, String expectedRegion, boolean isFips)
      throws IOException {

    String expected =
        "endpoint=" + endpoint + "; region=" + configuredRegion
        + "; expectedRegion=" + expectedRegion + "; isFips=" + isFips;
    LOG.info("Creating S3 client with {}", expected);
    EXPECTED_MESSAGE.set(expected);
    List<ExecutionInterceptor> interceptors = new ArrayList<>();
    interceptors.add(new RegionInterceptor(endpoint, expectedRegion, isFips));

    DefaultS3ClientFactory factory
        = new DefaultS3ClientFactory();
    factory.setConf(conf);
    S3ClientFactory.S3ClientCreationParameters parameters
        = new S3ClientFactory.S3ClientCreationParameters()
        .withCredentialSet(new AnonymousAWSCredentialsProvider())
        .withEndpoint(endpoint)
        .withMetrics(new EmptyS3AStatisticsContext()
            .newStatisticsFromAwsSdk())
        .withExecutionInterceptors(interceptors)
        .withRegion(configuredRegion)
        .withFipsEnabled(isFips);

    S3Client client = factory.createS3Client(
        getFileSystem().getUri(),
        parameters);
    return client;
  }
}
