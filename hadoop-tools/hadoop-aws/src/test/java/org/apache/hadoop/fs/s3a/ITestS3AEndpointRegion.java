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
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.awscore.AwsExecutionAttribute;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.PATH_STYLE_ACCESS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.io.IOUtils.closeStream;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;

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

  private static final String EU_WEST_2 = "eu-west-2";

  private static final String CN_NORTHWEST_1 = "cn-northwest-1";

  private static final String US_GOV_EAST_1 = "us-gov-east-1";

  /**
   * If anyone were ever to create a bucket with this UUID pair it would break the tests.
   */
  public static final String UNKNOWN_BUCKET = "23FA76D4-5F17-48B8-9D7D-9050269D0E40"
      + "-8281BAF2-DBCF-47AA-8A27-F2FA3589656A";

  private static final String EU_WEST_2_ENDPOINT = "s3.eu-west-2.amazonaws.com";

  private static final String CN_ENDPOINT = "s3.cn-northwest-1.amazonaws.com.cn";

  private static final String GOV_ENDPOINT = "s3-fips.us-gov-east-1.amazonaws.com";

  private static final String VPC_ENDPOINT = "vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com";

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

    S3Client client = createS3Client(conf, AWS_ENDPOINT_TEST, null, US_EAST_2);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }

  @Test
  public void testCentralEndpoint() throws Throwable {
    describe("Create a client with the central endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, CENTRAL_ENDPOINT, null, US_EAST_1);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }

  @Test
  public void testWithRegionConfig() throws Throwable {
    describe("Create a client with a configured region");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, null, EU_WEST_2, EU_WEST_2);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }


  public void testEUWest2Endpoint() throws Throwable {
    describe("Create a client with the eu west 2 endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, EU_WEST_2_ENDPOINT, null, EU_WEST_2);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }

  @Test
  public void testWithRegionAndEndpointConfig() throws Throwable {
    describe("Test that when both region and endpoint are configured, region takes precedence");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, EU_WEST_2_ENDPOINT, US_WEST_2, US_WEST_2);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }

  @Test
  public void testWithChinaEndpoint() throws Throwable {
    describe("Test with a china endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, CN_ENDPOINT, null, CN_NORTHWEST_1);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }

  @Test
  public void testWithGovCloudEndpoint() throws Throwable {
    describe("Test with a gov cloud endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, GOV_ENDPOINT, null, US_GOV_EAST_1);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }

  @Test
  @Ignore("Pending HADOOP-18938. S3A region logic to handle vpce and non standard endpoints")
  public void testWithVPCE() throws Throwable {
    describe("Test with vpc endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, VPC_ENDPOINT, null, US_WEST_2);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }

  class RegionInterceptor implements ExecutionInterceptor {
    private String endpoint;
    private String region;

    RegionInterceptor(String endpoint, String region) {
      this.endpoint = endpoint;
      this.region = region;
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context,
        ExecutionAttributes executionAttributes)  {

      if (endpoint != null) {
        Assertions.assertThat(
                executionAttributes.getAttribute(AwsExecutionAttribute.ENDPOINT_OVERRIDDEN))
            .describedAs("Endpoint not overridden").isTrue();

        Assertions.assertThat(
                executionAttributes.getAttribute(AwsExecutionAttribute.CLIENT_ENDPOINT).toString())
            .describedAs("There is an endpoint mismatch").isEqualTo("https://" + endpoint);
      } else {
        Assertions.assertThat(
                executionAttributes.getAttribute(AwsExecutionAttribute.ENDPOINT_OVERRIDDEN))
            .describedAs("Endpoint is overridden").isEqualTo(null);
      }

      Assertions.assertThat(
              executionAttributes.getAttribute(AwsExecutionAttribute.AWS_REGION).toString())
          .describedAs("Incorrect region set").isEqualTo(region);

      // We don't actually want to make a request, so exit early.
      throw AwsServiceException.builder().message("Exception thrown by interceptor").build();
    }
  }

  /**
   * Create an S3 client with the given conf and endpoint.
   * The region name must then match that of the expected
   * value.
   * @param conf configuration to use.
   * @param endpoint endpoint.
   * @param expectedRegion the region that should be set in the client.
   * @return the client.
   * @throws URISyntaxException parse problems.
   * @throws IOException IO problems
   */
  @SuppressWarnings("deprecation")
  private S3Client createS3Client(Configuration conf,
      String endpoint, String configuredRegion, String expectedRegion)
      throws IOException {

    List<ExecutionInterceptor> interceptors = new ArrayList<>();
    interceptors.add(new RegionInterceptor(endpoint, expectedRegion));

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
        .withRegion(configuredRegion);


    S3Client client = factory.createS3Client(
        getFileSystem().getUri(),
        parameters);
    return client;
  }
}
