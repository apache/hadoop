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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestArnResource extends HadoopTestBase {
  private final static Logger LOG = LoggerFactory.getLogger(TestArnResource.class);

  @Test
  public void parseAccessPointFromArn() throws IllegalArgumentException {
    describe("Parse AccessPoint ArnResource from arn string");

    String accessPoint = "testAp";
    String accountId = "123456789101";
    String[][] regionPartitionEndpoints = new String[][] {
        { Regions.EU_WEST_1.getName(), "aws", "s3-accesspoint.eu-west-1.amazonaws.com" },
        { Regions.US_GOV_EAST_1.getName(), "aws-us-gov", "s3-accesspoint.us-gov-east-1.amazonaws.com" },
        { Regions.CN_NORTH_1.getName(), "aws-cn", "s3-accesspoint.cn-north-1.amazonaws.com.cn" },
    };

    for (String[] testPair : regionPartitionEndpoints) {
      String region = testPair[0];
      String partition = testPair[1];
      String endpoint = testPair[2];

      // arn:partition:service:region:account-id:resource-type/resource-id
      String arn = String.format("arn:%s:s3:%s:%s:accesspoint/%s", partition, region, accountId,
          accessPoint);

      ArnResource resource = ArnResource.accessPointFromArn(arn);
      assertEquals("Arn does not match", arn, resource.getFullArn());
      assertEquals("Access Point name does not match", accessPoint, resource.getName());
      assertEquals("Account Id does not match", accountId, resource.getOwnerAccountId());
      assertEquals("Region does not match", region, resource.getRegion());
      assertEquals("Endpoint does not match", endpoint, resource.getEndpoint());
    }
  }

  @Test
  public void invalidARNsMustThrow() throws Exception {
    describe("Using an invalid ARN format must throw when initializing an ArnResource.");

    intercept(IllegalArgumentException.class, () ->
        ArnResource.accessPointFromArn("invalid:arn:resource"));
  }

  private void describe(String message) {
    LOG.info(message);
  }
}
