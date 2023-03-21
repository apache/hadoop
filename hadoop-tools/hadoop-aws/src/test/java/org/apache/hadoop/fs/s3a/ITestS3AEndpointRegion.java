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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_REGION_PROBE;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_301_MOVED_PERMANENTLY;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test to check correctness of S3A endpoint regions in
 * {@link DefaultS3ClientFactory}.
 */
public class ITestS3AEndpointRegion extends AbstractS3ATestBase {

  private static final String AWS_ENDPOINT_TEST = "test-endpoint";


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

    try  {
      fs.getBucketMetadata();
    } catch (S3Exception exception) {
      if (exception.statusCode() == SC_301_MOVED_PERMANENTLY) {
        Assert.fail(exception.toString());
      }
    }

    Assertions.assertThat(fs.getInstrumentation().getCounterValue(STORE_REGION_PROBE))
        .describedAs("Region is not configured, region probe should have been made").isEqualTo(1);
  }


  @Test
  public void testWithRegionConfig() throws IOException, URISyntaxException {
    Configuration conf = getConfiguration();
    conf.set(AWS_REGION, "us-east-2");

    S3AFileSystem fs = new S3AFileSystem();
    fs.initialize(new URI("s3a://landsat-pds"), conf);

    Assertions.assertThat(fs.getInstrumentation().getCounterValue(STORE_REGION_PROBE))
        .describedAs("Region is configured, region probe should not have been made").isEqualTo(0);
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
