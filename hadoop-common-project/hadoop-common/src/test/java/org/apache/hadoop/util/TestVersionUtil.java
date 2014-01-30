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
package org.apache.hadoop.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestVersionUtil {

  @Test
  public void testCompareVersions() {
    // Equal versions are equal.
    assertEquals(0, VersionUtil.compareVersions("2.0.0", "2.0.0"));
    assertEquals(0, VersionUtil.compareVersions("2.0.0a", "2.0.0a"));
    assertEquals(0, VersionUtil.compareVersions(
        "2.0.0-SNAPSHOT", "2.0.0-SNAPSHOT"));

    assertEquals(0, VersionUtil.compareVersions("1", "1"));
    assertEquals(0, VersionUtil.compareVersions("1", "1.0"));
    assertEquals(0, VersionUtil.compareVersions("1", "1.0.0"));

    assertEquals(0, VersionUtil.compareVersions("1.0", "1"));
    assertEquals(0, VersionUtil.compareVersions("1.0", "1.0"));
    assertEquals(0, VersionUtil.compareVersions("1.0", "1.0.0"));

    assertEquals(0, VersionUtil.compareVersions("1.0.0", "1"));
    assertEquals(0, VersionUtil.compareVersions("1.0.0", "1.0"));
    assertEquals(0, VersionUtil.compareVersions("1.0.0", "1.0.0"));

    assertEquals(0, VersionUtil.compareVersions("1.0.0-alpha-1", "1.0.0-a1"));
    assertEquals(0, VersionUtil.compareVersions("1.0.0-alpha-2", "1.0.0-a2"));
    assertEquals(0, VersionUtil.compareVersions("1.0.0-alpha1", "1.0.0-alpha-1"));

    assertEquals(0, VersionUtil.compareVersions("1a0", "1.0.0-alpha-0"));
    assertEquals(0, VersionUtil.compareVersions("1a0", "1-a0"));
    assertEquals(0, VersionUtil.compareVersions("1.a0", "1-a0"));
    assertEquals(0, VersionUtil.compareVersions("1.a0", "1.0.0-alpha-0"));

    // Assert that lower versions are lower, and higher versions are higher.
    assertExpectedValues("1", "2.0.0");
    assertExpectedValues("1.0.0", "2");
    assertExpectedValues("1.0.0", "2.0.0");
    assertExpectedValues("1.0", "2.0.0");
    assertExpectedValues("1.0.0", "2.0.0");
    assertExpectedValues("1.0.0", "1.0.0a");
    assertExpectedValues("1.0.0.0", "2.0.0");
    assertExpectedValues("1.0.0", "1.0.0-dev");
    assertExpectedValues("1.0.0", "1.0.1");
    assertExpectedValues("1.0.0", "1.0.2");
    assertExpectedValues("1.0.0", "1.1.0");
    assertExpectedValues("2.0.0", "10.0.0");
    assertExpectedValues("1.0.0", "1.0.0a");
    assertExpectedValues("1.0.2a", "1.0.10");
    assertExpectedValues("1.0.2a", "1.0.2b");
    assertExpectedValues("1.0.2a", "1.0.2ab");
    assertExpectedValues("1.0.0a1", "1.0.0a2");
    assertExpectedValues("1.0.0a2", "1.0.0a10");
    // The 'a' in "1.a" is not followed by digit, thus not treated as "alpha",
    // and treated larger than "1.0", per maven's ComparableVersion class
    // implementation.
    assertExpectedValues("1.0", "1.a");
    //The 'a' in "1.a0" is followed by digit, thus treated as "alpha-<digit>"
    assertExpectedValues("1.a0", "1.0");
    assertExpectedValues("1a0", "1.0");    
    assertExpectedValues("1.0.1-alpha-1", "1.0.1-alpha-2");    
    assertExpectedValues("1.0.1-beta-1", "1.0.1-beta-2");
    
    // Snapshot builds precede their eventual releases.
    assertExpectedValues("1.0-SNAPSHOT", "1.0");
    assertExpectedValues("1.0.0-SNAPSHOT", "1.0");
    assertExpectedValues("1.0.0-SNAPSHOT", "1.0.0");
    assertExpectedValues("1.0.0", "1.0.1-SNAPSHOT");
    assertExpectedValues("1.0.1-SNAPSHOT", "1.0.1");
    assertExpectedValues("1.0.1-SNAPSHOT", "1.0.2");
    
    assertExpectedValues("1.0.1-alpha-1", "1.0.1-SNAPSHOT");
    assertExpectedValues("1.0.1-beta-1", "1.0.1-SNAPSHOT");
    assertExpectedValues("1.0.1-beta-2", "1.0.1-SNAPSHOT");
  }
  
  private static void assertExpectedValues(String lower, String higher) {
    assertTrue(VersionUtil.compareVersions(lower, higher) < 0);
    assertTrue(VersionUtil.compareVersions(higher, lower) > 0);
  }
  
}
