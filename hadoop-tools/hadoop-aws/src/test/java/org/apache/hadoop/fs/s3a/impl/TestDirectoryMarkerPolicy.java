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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;

@RunWith(Parameterized.class)
public class TestDirectoryMarkerPolicy extends AbstractHadoopTestBase {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {
            DirectoryPolicy.MarkerPolicy.Delete,
            failIfInvoked,
            false, false
        },
        {
            DirectoryPolicy.MarkerPolicy.Keep,
            failIfInvoked,
            true, true
        },
        {
            DirectoryPolicy.MarkerPolicy.Authoritative,
            authPathOnly,
            false, true
        }});
  }

  private final DirectoryPolicyImpl retention;
  private final boolean expectNonAuthDelete;
  private final boolean expectAuthDelete;

  public TestDirectoryMarkerPolicy(
      final DirectoryPolicy.MarkerPolicy markerPolicy,
      final Predicate<Path> authoritativeness,
      final boolean expectNonAuthDelete,
      final boolean expectAuthDelete) {
    this.retention = retention(markerPolicy, authoritativeness);
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
  private DirectoryPolicyImpl retention(
      DirectoryPolicy.MarkerPolicy markerPolicy,
      Predicate<Path> authoritativeness) {
    Configuration c = new Configuration(false);
    c.set(DIRECTORY_MARKER_POLICY, markerPolicy.name());
    return new DirectoryPolicyImpl(c, authoritativeness);
  }

  private static final Predicate<Path> authPathOnly =
      (p) -> p.toUri().getPath().startsWith("/auth/");

  private static final Predicate<Path> failIfInvoked = (p) -> {
    throw new RuntimeException("failed");
  };

  private final Path nonAuthPath = new Path("s3a://bucket/nonauth/data");
  private final Path authPath = new Path("s3a://bucket/auth/data1");
  private final Path deepAuth = new Path("s3a://bucket/auth/d1/d2/data2");

  private void assertRetention(Path path, boolean retain) {
    Assertions.assertThat(retention.keepDirectoryMarkers(path))
        .describedAs("Retention of path %s by %s", path, retention)
        .isEqualTo(retain);
  }

  @Test
  public void testNonAuthPath() throws Throwable {
    assertRetention(nonAuthPath, expectNonAuthDelete);
  }

  @Test
  public void testAuthPath() throws Throwable {
    assertRetention(authPath, expectAuthDelete);
  }

  @Test
  public void testDeepAuthPath() throws Throwable {
    assertRetention(deepAuth, expectAuthDelete);
  }

}
