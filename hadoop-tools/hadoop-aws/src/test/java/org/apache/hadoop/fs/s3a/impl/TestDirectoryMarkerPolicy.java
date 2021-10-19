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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP;

/**
 * Unit tests for directory marker policies.
 */
@RunWith(Parameterized.class)
public class TestDirectoryMarkerPolicy extends AbstractHadoopTestBase {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {
            DirectoryPolicy.MarkerPolicy.Delete,
            FAIL_IF_INVOKED,
            false, false
        },
        {
            DirectoryPolicy.MarkerPolicy.Keep,
            FAIL_IF_INVOKED,
            true, true
        },
        {
            DirectoryPolicy.MarkerPolicy.Authoritative,
            AUTH_PATH_ONLY,
            false, true
        }
    });
  }

  private final DirectoryPolicy directoryPolicy;

  private final boolean expectNonAuthDelete;

  private final boolean expectAuthDelete;

  public TestDirectoryMarkerPolicy(
      final DirectoryPolicy.MarkerPolicy markerPolicy,
      final Predicate<Path> authoritativeness,
      final boolean expectNonAuthDelete,
      final boolean expectAuthDelete) {
    this.directoryPolicy = newPolicy(markerPolicy, authoritativeness);
    this.expectNonAuthDelete = expectNonAuthDelete;
    this.expectAuthDelete = expectAuthDelete;
  }

  /**
   * Create a new retention policy.
   * @param markerPolicy policy option
   * @param authoritativeness predicate for determining if
   * a path is authoritative.
   * @return the retention policy.
   */
  private DirectoryPolicy newPolicy(
      DirectoryPolicy.MarkerPolicy markerPolicy,
      Predicate<Path> authoritativeness) {
    return new DirectoryPolicyImpl(markerPolicy, authoritativeness);
  }

  private static final Predicate<Path> AUTH_PATH_ONLY =
      (p) -> p.toUri().getPath().startsWith("/auth/");

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
    Assertions.assertThat(directoryPolicy.keepDirectoryMarkers(path))
        .describedAs("Retention of path %s by %s", path, directoryPolicy)
        .isEqualTo(retain);
  }

  /**
   * Assert that a path has a capability.
   */
  private void assertPathCapability(Path path,
      String capability,
      boolean outcome) {
    Assertions.assertThat(directoryPolicy)
        .describedAs("%s support for capability %s by path %s"
                + " expected as %s",
            directoryPolicy, capability, path, outcome)
        .matches(p -> p.hasPathCapability(path, capability) == outcome,
            "pathCapability");
  }

  @Test
  public void testNonAuthPath() throws Throwable {
    assertMarkerRetention(nonAuthPath, expectNonAuthDelete);
    assertPathCapability(nonAuthPath,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE,
        !expectNonAuthDelete);
    assertPathCapability(nonAuthPath,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP,
        expectNonAuthDelete);
  }

  @Test
  public void testAuthPath() throws Throwable {
    assertMarkerRetention(authPath, expectAuthDelete);
    assertPathCapability(authPath,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE,
        !expectAuthDelete);
    assertPathCapability(authPath,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP,
        expectAuthDelete);
  }

  @Test
  public void testDeepAuthPath() throws Throwable {
    assertMarkerRetention(deepAuth, expectAuthDelete);
    assertPathCapability(deepAuth,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE,
        !expectAuthDelete);
    assertPathCapability(deepAuth,
        STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP,
        expectAuthDelete);
  }

}
