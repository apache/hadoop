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
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.awscore.AwsExecutionAttribute;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.BUCKET_REGION_HEADER;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_REGION_PROBE;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_301_MOVED_PERMANENTLY;

/**
 * Test to check correctness of S3A endpoint regions in
 * {@link DefaultS3ClientFactory}.
 */
public class ITestS3AEndpointRegion extends AbstractS3ATestBase {

  private static String AWS_REGION_TEST;
  private static final String AWS_ENDPOINT_TEST = "test-endpoint";
  public static final String MARS_NORTH_2 = "mars-north-2";


  /**
   * Test to verify that not setting the region config, will lead to the client factory making
   * a HEAD bucket call to configure the correct region. If an incorrect region is set, the HEAD
   * bucket call in this test will raise an exception.
   */
  @Test
  public void testWithoutRegionConfig() throws IOException {
    Configuration conf = getConfiguration();
    String bucket = getFileSystem().getBucket();
    conf.unset(String.format("fs.s3a.bucket.%s.endpoint.region", bucket));
    conf.unset(AWS_REGION);

    S3AFileSystem fs = new S3AFileSystem();
    fs.initialize(getFileSystem().getUri(), conf);
    S3Client s3Client = fs.getAmazonS3ClientForTesting("testWithoutRegionConfig");

    try (AuditSpan span = span()) {
      s3Client.headBucket(HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build());
    } catch (S3Exception exception) {
      if (exception.statusCode() == SC_301_MOVED_PERMANENTLY) {
        List<String> bucketRegion =
            exception.awsErrorDetails().sdkHttpResponse().headers().get(BUCKET_REGION_HEADER);
        Assert.fail("Region not configured correctly, expected region: " + bucketRegion);
      }
    }
  }

  @Test
  public void testRegionCache() throws IOException, URISyntaxException {
    Configuration conf = getConfiguration();
    conf.unset(AWS_REGION);
    conf.unset("fs.s3a.bucket.landsat-pds.endpoint.region");
    S3AFileSystem fs = new S3AFileSystem();

    fs.initialize(new URI("s3a://landsat-pds"), conf);

    Assertions.assertThat(fs.getInstrumentation().getCounterValue(STORE_REGION_PROBE))
        .describedAs("Incorrect number of calls made to get bucket region").isEqualTo(1);

    fs.initialize(new URI("s3a://landsat-pds"), conf);

    // value should already be cached.
    Assertions.assertThat(fs.getInstrumentation().getCounterValue(STORE_REGION_PROBE))
        .describedAs("Incorrect number of calls made to get bucket region").isEqualTo(0);
  }

  @Test
  public void testInvalidRegionDefaultEndpoint() throws Throwable {
    describe("Create a client with an invalid region and the default endpoint");
    Configuration conf = getConfiguration();
    // we are making a big assumption about the timetable for AWS
    // region rollout.
    // if this test ever fails because this region now exists
    // -congratulations!
    conf.set(AWS_REGION, MARS_NORTH_2);
    AWS_REGION_TEST = MARS_NORTH_2;
    createMarsNorth2Client(conf);
  }

  @Test
  public void testEndpointOverride() throws Throwable {
    describe("Create a client with no region and the default endpoint");
    Configuration conf = getConfiguration();

    String bucketSpecificRegion =
        conf.get(String.format("fs.s3a.bucket.%s.endpoint.region", getFileSystem().getBucket()));

    if (bucketSpecificRegion != null && !bucketSpecificRegion.isEmpty()) {
      conf.set(AWS_REGION, bucketSpecificRegion);
    }

    AWS_REGION_TEST = conf.get(AWS_REGION);

    S3Client client = createS3Client(conf, AWS_ENDPOINT_TEST);

    try {
      client.headBucket(HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build());
    } catch (AwsServiceException exception) {
      // Expected to be thrown by interceptor, do nothing.
    }
  }

  /**
   * Create an S3 client bonded to an invalid region;
   * verify that calling {@code getRegion()} triggers
   * a failure.
   * @param conf configuration to use in the building.
   */
  private void createMarsNorth2Client(Configuration conf) throws Exception {
    S3Client client = createS3Client(conf, "");

    try {
      client.headBucket(HeadBucketRequest.builder().bucket(getFileSystem().getBucket()).build());
    } catch (AwsServiceException exception) {
      // Expected to be thrown by interceptor, do nothing.
    }
  }

  class RegionInterceptor implements ExecutionInterceptor {
    private boolean endpointOverridden;

    RegionInterceptor(boolean endpointOverridden) {
      this.endpointOverridden = endpointOverridden;
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context,
        ExecutionAttributes executionAttributes)  {

      Assertions.assertThat(executionAttributes.getAttribute(AwsExecutionAttribute.AWS_REGION))
                    .describedAs("There is a region mismatch")
                    .isEqualTo(Region.of(AWS_REGION_TEST));

      if (endpointOverridden) {
        Assertions.assertThat(
                executionAttributes.getAttribute(AwsExecutionAttribute.ENDPOINT_OVERRIDDEN))
            .describedAs("Endpoint not overridden").isTrue();

        Assertions.assertThat(
                executionAttributes.getAttribute(AwsExecutionAttribute.CLIENT_ENDPOINT).toString())
            .describedAs("There is an endpoint mismatch").isEqualTo("https://" + AWS_ENDPOINT_TEST);
      }

      // We don't actually want to make a request, so exit early.
      throw AwsServiceException.builder().build();
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
