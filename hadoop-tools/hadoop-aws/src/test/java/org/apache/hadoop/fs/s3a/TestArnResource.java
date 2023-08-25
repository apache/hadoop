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

import com.amazonaws.regions.Regions;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Verifies the mapping of ARN declaration of resource to the associated
 * access point.
 * The region mapping assertions have been brittle to changes across AWS SDK
 * versions to only verify partial matches, rather than the FQDN of the
 * endpoints.
 *
 */
public class TestArnResource extends HadoopTestBase {
  private final static Logger LOG = LoggerFactory.getLogger(TestArnResource.class);

  private final static String MOCK_ACCOUNT = "123456789101";

  @Test
  public void parseAccessPointFromArn() throws IllegalArgumentException {
    describe("Parse AccessPoint ArnResource from arn string");

    String accessPoint = "testAp";
    String[][] regionPartitionEndpoints = new String[][] {
        {Regions.EU_WEST_1.getName(), "aws"},
        {Regions.US_GOV_EAST_1.getName(), "aws-us-gov"},
        {Regions.CN_NORTH_1.getName(), "aws-cn"},
    };

    for (String[] testPair : regionPartitionEndpoints) {
      String region = testPair[0];
      String partition = testPair[1];

      ArnResource resource = getArnResourceFrom(partition, "s3", region, MOCK_ACCOUNT, accessPoint);
      assertEquals("Access Point name does not match", accessPoint, resource.getName());
      assertEquals("Account Id does not match", MOCK_ACCOUNT, resource.getOwnerAccountId());
      assertEquals("Region does not match", region, resource.getRegion());
    }
  }

  @Test
  public void makeSureS3EndpointHasTheCorrectFormat() {
    // Access point (AP) endpoints are different from S3 bucket endpoints, thus when using APs the
    // endpoints for the client are modified. This test makes sure endpoint is set up correctly.
    ArnResource accessPoint = getArnResourceFrom("aws", "s3", "eu-west-1", MOCK_ACCOUNT,
        "test");
    String expected = "s3-accesspoint.eu-west-1.amazonaws.com";

    Assertions.assertThat(accessPoint.getEndpoint())
        .describedAs("Endpoint has invalid format. Access Point requests will not work")
        .isEqualTo(expected);
  }

  @Test
  public void makeSureS3OutpostsEndpointHasTheCorrectFormat() {
    // Access point (AP) endpoints are different from S3 bucket endpoints, thus when using APs the
    // endpoints for the client are modified. This test makes sure endpoint is set up correctly.
    ArnResource accessPoint = getArnResourceFrom("aws", "s3-outposts", "eu-west-1", MOCK_ACCOUNT,
        "test");
    String expected = "s3-outposts.eu-west-1.amazonaws.com";

    Assertions.assertThat(accessPoint.getEndpoint())
        .describedAs("Endpoint has invalid format. Access Point requests will not work")
        .isEqualTo(expected);
  }

  @Test
  public void invalidARNsMustThrow() throws Exception {
    describe("Using an invalid ARN format must throw when initializing an ArnResource.");

    intercept(IllegalArgumentException.class, () ->
        ArnResource.accessPointFromArn("invalid:arn:resource"));
  }

  /**
   * Create an {@link ArnResource} from string components
   * @param partition - partition for ARN
   * @param service - service for ARN
   * @param region - region for ARN
   * @param accountId - accountId for ARN
   * @param resourceName - ARN resource name
   * @return ArnResource described by its properties
   */
  private ArnResource getArnResourceFrom(String partition, String service, String region, String accountId,
      String resourceName) {
    // arn:partition:service:region:account-id:resource-type/resource-id
    String arn = String.format("arn:%s:%s:%s:%s:accesspoint/%s", partition, service, region, accountId,
        resourceName);

    return ArnResource.accessPointFromArn(arn);
  }

  private void describe(String message) {
    LOG.info(message);
  }
}
