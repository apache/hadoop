/**
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

package org.apache.hadoop.mapreduce;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.junit.Test;

/**
 * Test class for MRResourceVisibility.
 */
public class TestMRResourceVisibility {

  @Test
  public void testGetVisibility() {
    // lower case
    MRResourceVisibility visibility =
        MRResourceVisibility.getVisibility("public");
    assertTrue(visibility.name().equals(MRResourceVisibility.PUBLIC.name()));

    // Upper case
    visibility =
        MRResourceVisibility.getVisibility("PUBLIC");
    assertTrue(visibility.name().equals(MRResourceVisibility.PUBLIC.name()));
  }

  @Test
  public void testGetYarnLocalResourceVisibility() {
    testGetYarnLocalResourceInternal(
        MRResourceVisibility.PUBLIC, LocalResourceVisibility.PUBLIC);
    testGetYarnLocalResourceInternal(
        MRResourceVisibility.APPLICATION, LocalResourceVisibility.APPLICATION);
    testGetYarnLocalResourceInternal(
        MRResourceVisibility.PRIVATE, LocalResourceVisibility.PRIVATE);
  }

  private void testGetYarnLocalResourceInternal(
      MRResourceVisibility input, LocalResourceVisibility expectOutput) {
    LocalResourceVisibility vis =
        MRResourceVisibility.getYarnLocalResourceVisibility(input);
    assertTrue(vis.name().equals(expectOutput.name()));
  }
}
