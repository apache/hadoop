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
import static org.apache.hadoop.fs.s3a.Statistic.STORE_REGION_PROBE;
import static org.apache.hadoop.io.IOUtils.closeStream;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test to check correctness of S3A endpoint regions in
 * {@link DefaultS3ClientFactory}.
 */
public class ITestS3AEndpointRegion extends AbstractS3ATestBase {

  private static final String AWS_ENDPOINT_TEST = "test-endpoint";

  private static final String USW_2_BUCKET = "landsat-pds";

  public static final String USW_2_STORE = "s3a://" + USW_2_BUCKET;

  /**
   * If anyone were ever to create a bucket with this UUID pair it would break the tests.
   */
  public static final String UNKNOWN_BUCKET = "23FA76D4-5F17-48B8-9D7D-9050269D0E40"
      + "-8281BAF2-DBCF-47AA-8A27-F2FA3589656A";

  /**
   * New FS instance which will be closed in teardown.
   */
  private S3AFileSystem newFS;

  @Override
  public void teardown() throws Exception {
    closeStream(newFS);
    super.teardown();
  }

  /**
   * Test to verify that not setting the region config, will lead to the client factory making
   * a HEAD bucket call to configure the correct region. If an incorrect region is set, the HEAD
   * bucket call in this test will raise an exception.
   */
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
    assertRegionProbeCount(1);
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
    assertRegionProbeCount(1);
  }


  @Test
  public void testWithRegionConfig() throws IOException, URISyntaxException {
    describe("Verify that region lookup is skipped if the region property is set");
    Configuration conf = getConfiguration();
    removeBaseAndBucketOverrides(conf, AWS_REGION, PATH_STYLE_ACCESS);

    conf.set(AWS_REGION, "us-east-2");

    newFS = new S3AFileSystem();
    newFS.initialize(new URI(USW_2_STORE), conf);
    assertRegionProbeCount(0);
  }

  @Test
  public void testRegionCache() throws IOException, URISyntaxException {
    describe("Verify that region lookup is cached on the second attempt");
    Configuration conf = getConfiguration();
    removeBaseAndBucketOverrides(USW_2_BUCKET, conf, AWS_REGION, PATH_STYLE_ACCESS);

    newFS = new S3AFileSystem();

    newFS.initialize(new URI(USW_2_STORE), conf);

    assertRegionProbeCount(1);
    closeStream(newFS);

    // create a new instance
    newFS = new S3AFileSystem();
    newFS.initialize(new URI(USW_2_STORE), conf);

    // value should already be cached.
    assertRegionProbeCount(0);
  }

  private void assertRegionProbeCount(final int expected) {
    Assertions.assertThat(newFS.getInstrumentation().getCounterValue(STORE_REGION_PROBE))
        .describedAs("Incorrect number of calls made to get bucket region").isEqualTo(expected);
  }

  @Test
  public void testEndpointOverride() throws Throwable {
    describe("Create a client with no region and the default endpoint");
    Configuration conf = getConfiguration();

    S3Client client = createS3Client(conf, AWS_ENDPOINT_TEST);

    intercept(AwsServiceException.class, "Exception thrown by interceptor", () -> client.headBucket(
        HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build()));
  }


  class RegionInterceptor implements ExecutionInterceptor {
    private boolean endpointOverridden;

    RegionInterceptor(boolean endpointOverridden) {
      this.endpointOverridden = endpointOverridden;
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context,
        ExecutionAttributes executionAttributes)  {

      if (endpointOverridden) {
        Assertions.assertThat(
                executionAttributes.getAttribute(AwsExecutionAttribute.ENDPOINT_OVERRIDDEN))
            .describedAs("Endpoint not overridden").isTrue();

        Assertions.assertThat(
                executionAttributes.getAttribute(AwsExecutionAttribute.CLIENT_ENDPOINT).toString())
            .describedAs("There is an endpoint mismatch").isEqualTo("https://" + AWS_ENDPOINT_TEST);
      }

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
   * @return the client.
   * @throws URISyntaxException parse problems.
   * @throws IOException IO problems
   */
  @SuppressWarnings("deprecation")
  private S3Client createS3Client(Configuration conf,
      String endpoint)
      throws IOException {

    boolean endpointOverridden = false;

    if (endpoint != null && !endpoint.isEmpty()) {
      endpointOverridden = true;
    }

    List<ExecutionInterceptor> interceptors = new ArrayList<>();
    interceptors.add(new RegionInterceptor(endpointOverridden));

    DefaultS3ClientFactory factory
        = new DefaultS3ClientFactory();
    factory.setConf(conf);
    S3ClientFactory.S3ClientCreationParameters parameters
        = new S3ClientFactory.S3ClientCreationParameters()
        .withCredentialSet(new AnonymousAWSCredentialsProvider())
        .withEndpoint(endpoint)
        .withMetrics(new EmptyS3AStatisticsContext()
            .newStatisticsFromAwsSdk())
        .withExecutionInterceptors(interceptors);

    S3Client client = factory.createS3Client(
        getFileSystem().getUri(),
        parameters);
    return client;
  }
}
