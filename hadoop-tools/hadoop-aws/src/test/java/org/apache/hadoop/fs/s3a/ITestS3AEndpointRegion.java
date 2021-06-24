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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.util.AwsHostNameUtils;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CENTRAL_REGION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ENDPOINT;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.AWS_REGION_SYSPROP;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test to check correctness of S3A endpoint regions in
 * {@link DefaultS3ClientFactory}.
 */
public class ITestS3AEndpointRegion extends AbstractS3ATestBase {

  private static final String AWS_REGION_TEST = "test-region";
  private static final String AWS_ENDPOINT_TEST = "test-endpoint";
  private static final String AWS_ENDPOINT_TEST_WITH_REGION =
      "test-endpoint.some-region.amazonaws.com";
  public static final String MARS_NORTH_2 = "mars-north-2";

  /**
   * Test to verify that setting a region with the config would bypass the
   * construction of region from endpoint.
   */
  @Test
  public void testWithRegionConfig() {
    getFileSystem().getConf().set(AWS_REGION, AWS_REGION_TEST);

    //Creating an endpoint config with a custom endpoint.
    AwsClientBuilder.EndpointConfiguration epr = createEpr(AWS_ENDPOINT_TEST,
        getFileSystem().getConf().getTrimmed(AWS_REGION));
    //Checking if setting region config bypasses the endpoint region.
    Assertions.assertThat(epr.getSigningRegion())
        .describedAs("There is a region mismatch")
        .isEqualTo(getFileSystem().getConf().get(AWS_REGION));
  }

  /**
   * Test to verify that not setting the region config, would lead to using
   * endpoint to construct the region.
   */
  @Test
  public void testWithoutRegionConfig() {
    getFileSystem().getConf().unset(AWS_REGION);

    //Creating an endpoint config with a custom endpoint containing a region.
    AwsClientBuilder.EndpointConfiguration eprRandom =
        createEpr(AWS_ENDPOINT_TEST_WITH_REGION,
            getFileSystem().getConf().getTrimmed(AWS_REGION));
    String regionFromEndpoint =
        AwsHostNameUtils
            .parseRegionFromAwsPartitionPattern(AWS_ENDPOINT_TEST_WITH_REGION);
    //Checking if not setting region config leads to constructing the region
    // from endpoint.
    Assertions.assertThat(eprRandom.getSigningRegion())
        .describedAs("There is a region mismatch")
        .isNotEqualTo(getFileSystem().getConf().get(AWS_REGION))
        .isEqualTo(regionFromEndpoint);
  }

  /**
   * Method to create EndpointConfiguration using an endpoint.
   *
   * @param endpoint the endpoint to be used for EndpointConfiguration creation.
   * @return an instance of EndpointConfiguration.
   */
  private AwsClientBuilder.EndpointConfiguration createEpr(String endpoint,
      String awsRegion) {
    return DefaultS3ClientFactory.createEndpointConfiguration(endpoint,
        new ClientConfiguration(), awsRegion);
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
    createMarsNorth2Client(conf);
  }

  @Test
  public void testUnsetRegionDefaultEndpoint() throws Throwable {
    describe("Create a client with no region and the default endpoint");
    Configuration conf = getConfiguration();
    conf.unset(AWS_REGION);
    createS3Client(conf, DEFAULT_ENDPOINT, AWS_S3_CENTRAL_REGION);
  }

  /**
   * By setting the system property {@code "aws.region"} we can
   * guarantee that the SDK region resolution chain will always succeed
   * (and fast).
   * Clearly there is no validation of the region during the build process.
   */
  @Test
  public void testBlankRegionTriggersSDKResolution() throws Throwable {
    describe("Create a client with a blank region and the default endpoint."
        + " This will trigger the SDK Resolution chain");
    Configuration conf = getConfiguration();
    conf.set(AWS_REGION, "");
    System.setProperty(AWS_REGION_SYSPROP, MARS_NORTH_2);
    try {
      createMarsNorth2Client(conf);
    } finally {
      System.clearProperty(AWS_REGION_SYSPROP);
    }
  }

  /**
   * Create an S3 client bonded to an invalid region;
   * verify that calling {@code getRegion()} triggers
   * a failure.
   * @param conf configuration to use in the building.
   */
  private void createMarsNorth2Client(Configuration conf) throws Exception {
    AmazonS3 client = createS3Client(conf, DEFAULT_ENDPOINT, MARS_NORTH_2);
    intercept(IllegalArgumentException.class, MARS_NORTH_2, client::getRegion);
  }

  /**
   * Create an S3 client with the given conf and endpoint.
   * The region name must then match that of the expected
   * value.
   * @param conf configuration to use.
   * @param endpoint endpoint.
   * @param expectedRegion expected region
   * @return the client.
   * @throws URISyntaxException parse problems.
   * @throws IOException IO problems
   */
  private AmazonS3 createS3Client(Configuration conf,
      String endpoint,
      String expectedRegion)
      throws URISyntaxException, IOException {

    DefaultS3ClientFactory factory
        = new DefaultS3ClientFactory();
    factory.setConf(conf);
    S3ClientFactory.S3ClientCreationParameters parameters
        = new S3ClientFactory.S3ClientCreationParameters()
        .withCredentialSet(new AnonymousAWSCredentialsProvider())
        .withEndpoint(endpoint)
        .withMetrics(new EmptyS3AStatisticsContext()
            .newStatisticsFromAwsSdk());
    AmazonS3 client = factory.createS3Client(
        new URI("s3a://localhost/"),
        parameters);
    Assertions.assertThat(client.getRegionName())
        .describedAs("Client region name")
        .isEqualTo(expectedRegion);
    return client;
  }
}
