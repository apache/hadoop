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

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP;

/**
 * Unit tests for directory marker policies.
 * <p></p>
 * As this FS only supports "delete", this is more minimal test
 * suite than on later versions.
 * <p></p>
 * It helps ensure that there aren't unexpected problems if the site configuration
 * asks for retention of some form.
 */
@RunWith(Parameterized.class)
public class TestDirectoryMarkerPolicy extends HadoopTestBase {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {
            DirectoryPolicy.MarkerPolicy.Delete,
        },
        {
            DirectoryPolicy.MarkerPolicy.Keep,
        },
        {
            DirectoryPolicy.MarkerPolicy.Authoritative,
        }
    });
  }

  private final DirectoryPolicy directoryPolicy;

  private static final boolean EXPECT_MARKER_RETENTION = false;

  public TestDirectoryMarkerPolicy(
      final DirectoryPolicy.MarkerPolicy markerPolicy) {
    this.directoryPolicy = newPolicy(markerPolicy);
  }

  /**
   * Create a new retention policy.
   * @param markerPolicy policy option
   * @return the retention policy.
   */
  private DirectoryPolicy newPolicy(
      DirectoryPolicy.MarkerPolicy markerPolicy) {
    return new DirectoryPolicyImpl(markerPolicy, FAIL_IF_INVOKED);
  }

  private static final Predicate<Path> FAIL_IF_INVOKED = (p) -> {
    throw new RuntimeException("failed");
  };

  private final Path nonAuthPath = new Path("s3a://bucket/nonauth/data");

  private final Path authPath = new Path("s3a://bucket/auth/data1");

  private final Path deepAuth = new Path("s3a://bucket/auth/d1/d2/data2");

  /**
   * Assert that a path has a retention outcome.
   * @param path path
   * @param retain should the marker be retained
   */
  private void assertMarkerRetention(Path path, boolean retain) {
    assertEquals("Retention of path " + path + " by " + directoryPolicy,
        retain,
        directoryPolicy.keepDirectoryMarkers(path));
  }

  /**
   * Assert that a path has a capability.
   */
  private void assertPathCapability(Path path,
      String capability,
      boolean outcome) {
    assertEquals(String.format(
        "%s support for capability %s by path %s expected as %s",
        directoryPolicy, capability, path, outcome),
        outcome,
        directoryPolicy.hasPathCapability(path, capability));
  }

  @Test
  public void testNonAuthPath() throws Throwable {
    assertMarkerRetention(nonAuthPath, EXPECT_MARKER_RETENTION);
    assertPathCapability(nonAuthPath,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE,
        !EXPECT_MARKER_RETENTION);
    assertPathCapability(nonAuthPath,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP,
        EXPECT_MARKER_RETENTION);
  }

  @Test
  public void testAuthPath() throws Throwable {
    assertMarkerRetention(authPath, EXPECT_MARKER_RETENTION);
    assertPathCapability(authPath,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE,
        !EXPECT_MARKER_RETENTION);
    assertPathCapability(authPath,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP,
        EXPECT_MARKER_RETENTION);
  }

  @Test
  public void testDeepAuthPath() throws Throwable {
    assertMarkerRetention(deepAuth, EXPECT_MARKER_RETENTION);
    assertPathCapability(deepAuth,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE,
        !EXPECT_MARKER_RETENTION);
    assertPathCapability(deepAuth,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP,
        EXPECT_MARKER_RETENTION);
  }

  @Test
  public void testInstantiate() throws Throwable {
    Configuration conf = new Configuration(false);
    conf.set(DIRECTORY_MARKER_POLICY,
        directoryPolicy.getMarkerPolicy().getOptionName());
    DirectoryPolicy policy = DirectoryPolicyImpl.getDirectoryPolicy(
        conf);
    assertEquals(DirectoryPolicyImpl.DELETE, policy);

  }
}
