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

import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.impl.NetworkBinding.fixBucketRegion;

/**
 * Unit tests related to the {@link NetworkBinding} class.
 */
public class TestNeworkBinding extends HadoopTestBase {

  @Test
  public void testUSEast() throws Throwable {
    assertRegionFixup("us-east-1", "us-east-1");
  }

  @Test
  public void testUSWest() throws Throwable {
    assertRegionFixup("us-west-2", "us-west-2");
  }

  @Test
  public void testRegionUStoUSEast() throws Throwable {
    assertRegionFixup("US", "us-east-1");
  }

  private static void assertRegionFixup(String region, String expected) {
    Assertions.assertThat(fixBucketRegion(region))
        .describedAs("Fixup of %s", region)
        .isEqualTo(expected);
  }
}
