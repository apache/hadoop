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

package org.apache.hadoop.fs.s3a.impl;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.impl.S3ExpressStorage.isS3ExpressStore;

/**
 * Test S3 Express Storage methods.
 */
public class TestS3ExpressStorage extends AbstractHadoopTestBase {

  private static final String S3EXPRESS_BUCKET = "bucket--usw2-az2--x-s3";
  private static final String AZ = "usw2-az2";

  @Test
  public void testS3ExpressStateDefaultEndpoint() {
    assertS3ExpressState(S3EXPRESS_BUCKET, true, "");
    assertS3ExpressState("bucket", false, "");
  }

  @Test
  public void testS3ExpressStateAwsRegions() {
    assertS3ExpressState(S3EXPRESS_BUCKET, true, "s3.amazonaws.com");
    assertS3ExpressState(S3EXPRESS_BUCKET, true, "s3.cn-northwest-1.amazonaws.com.cn");
    assertS3ExpressState(S3EXPRESS_BUCKET, true, "s3-fips.us-gov-east-1.amazonaws.com");
    assertS3ExpressState(S3EXPRESS_BUCKET, true,
        "s3-accesspoint-fips.dualstack.us-east-1.amazonaws.com");
    assertS3ExpressState(S3EXPRESS_BUCKET, true, "s3.ca-central-1.amazonaws.com");
  }

  @Test
  public void testS3ExpressStateThirdPartyStore() {
    assertS3ExpressState(S3EXPRESS_BUCKET, false, "storeendpoint.example.com");
  }

  private void assertS3ExpressState(final String bucket, final boolean expected, String endpoint) {
    Assertions.assertThat(isS3ExpressStore(bucket, endpoint))
        .describedAs("isS3ExpressStore(%s) with endpoint %s", bucket, endpoint)
        .isEqualTo(expected);
  }

}
