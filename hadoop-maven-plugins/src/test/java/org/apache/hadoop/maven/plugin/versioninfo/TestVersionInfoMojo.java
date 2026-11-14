/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.maven.plugin.versioninfo;

import java.time.Instant;

import org.apache.maven.plugin.MojoExecutionException;

import org.assertj.core.api.AbstractInstantAssert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.maven.plugin.versioninfo.VersionInfoMojo.parseBuildTime;

/**
 * Unit tests for build timestamp parsing in {@link VersionInfoMojo}.
 */
class TestVersionInfoMojo {

  private static final Instant EXPECTED =
      Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void testUnsetValues() throws Exception {
    assertParsedBuildTime(null).isNull();
    assertParsedBuildTime("").isNull();
    assertParsedBuildTime("-").isNull();
  }

  private static AbstractInstantAssert<?> assertParsedBuildTime(final String v)
      throws MojoExecutionException {
    return Assertions.assertThat(parseBuildTime(v)).describedAs("parsed build time '%s'", v);
  }

  @Test
  void testExpectedSeconds() throws Exception {
    assertParsedBuildTime("1767225600").isEqualTo(EXPECTED);
  }

  @Test
  void testEpochSeconds() throws Exception {
    assertParsedBuildTime("0000000000").isEqualTo(Instant.EPOCH);
  }

  @Test
  void testIso8601UTC() throws Exception {
    assertParsedBuildTime("2026-01-01T00:00:00Z").isEqualTo(EXPECTED);
  }

  @Test
  void testIso8601() throws Exception {
    assertParsedBuildTime("2026-01-01T01:00:00+01:00").isEqualTo(EXPECTED);
  }

  @Test
  void testUnparseableValueIsReported() {
    Assertions.assertThatThrownBy(() -> parseBuildTime("last thursday"))
        .isInstanceOf(MojoExecutionException.class)
        .hasMessageContaining("last thursday");
  }

  @Test
  void testMissingZoneIsRejected() {
    // no offset means no instant; better to fail than to guess a zone
    Assertions.assertThatThrownBy(() -> parseBuildTime("2026-01-01T00:00:00"))
        .isInstanceOf(MojoExecutionException.class);
  }
}
