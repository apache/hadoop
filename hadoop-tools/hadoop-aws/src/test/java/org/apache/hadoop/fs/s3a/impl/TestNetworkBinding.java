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

import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.impl.NetworkBinding.fixBucketRegion;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests related to the {@link NetworkBinding} class.
 */
public class TestNetworkBinding extends AbstractHadoopTestBase {

  private static final String US_EAST_1 = "us-east-1";

  private static final String US_WEST_2 = "us-west-2";

  @Test
  public void testUSEast() {
    assertRegionFixup(US_EAST_1, US_EAST_1);
  }

  @Test
  public void testUSWest() {
    assertRegionFixup(US_WEST_2, US_WEST_2);
  }

  @Test
  public void testRegionUStoUSEast() {
    assertRegionFixup("US", US_EAST_1);
  }

  @Test
  public void testRegionNullToUSEast() {
    assertRegionFixup(null, US_EAST_1);
  }

  private static void assertRegionFixup(String region, String expected) {
    assertThat(fixBucketRegion(region))
        .describedAs("Fixup of %s", region)
        .isEqualTo(expected);
  }
}
