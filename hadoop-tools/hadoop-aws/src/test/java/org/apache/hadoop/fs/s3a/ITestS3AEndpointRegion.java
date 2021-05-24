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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.util.AwsHostNameUtils;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;

/**
 * Test to check correctness of S3A endpoint regions in
 * {@link DefaultS3ClientFactory}.
 */
public class ITestS3AEndpointRegion extends AbstractS3ATestBase {

  private static final String AWS_REGION_TEST = "test-region";
  private static final String AWS_ENDPOINT_TEST = "test-endpoint";
  private static final String AWS_ENDPOINT_TEST_WITH_REGION =
      "test-endpoint.some-region.amazonaws.com";

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
}
