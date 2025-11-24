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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

public class TestS3AEndpointParsing extends AbstractS3AMockTest {

    private static final String VPC_ENDPOINT = "vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com";
    private static final String NON_VPC_ENDPOINT = "s3.eu-west-1.amazonaws.com";
    private static final String US_WEST_2 = "us-west-2";
    private static final String EU_WEST_1 = "eu-west-1";

    @Test
    public void testVPCEndpoint() {
        Region region = DefaultS3ClientFactory.getS3RegionFromEndpoint(VPC_ENDPOINT, false);
        Assertions.assertThat(region).isEqualTo(Region.of(US_WEST_2));
    }

    @Test
    public void testNonVPCEndpoint() {
        Region region = DefaultS3ClientFactory.getS3RegionFromEndpoint(NON_VPC_ENDPOINT, false);
        Assertions.assertThat(region).isEqualTo(Region.of(EU_WEST_1));
    }
}
