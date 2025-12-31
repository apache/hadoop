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

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SDK_REGION;
import static org.apache.hadoop.fs.s3a.impl.RegionResolution.ERROR_ENDPOINT_WITH_FIPS;
import static org.apache.hadoop.fs.s3a.impl.RegionResolution.calculateRegion;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test region resolution logic in {@link RegionResolution}.
 * These are based on {@code ITestS3AEndpointRegion}.
 */
public class TestRegionResolution extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegionResolution.class);

  private static final String US_EAST_1 = "us-east-1";

  private static final String US_EAST_2 = "us-east-2";

  private static final String US_WEST_2 = "us-west-2";

  private static final String EU_WEST_2 = "eu-west-2";

  private static final String CN_NORTHWEST_1 = "cn-northwest-1";

  private static final String US_GOV_EAST_1 = "us-gov-east-1";

  private static final String EU_WEST_2_ENDPOINT = "s3.eu-west-2.amazonaws.com";

  private static final String CN_ENDPOINT = "s3.cn-northwest-1.amazonaws.com.cn";

  private static final String GOV_ENDPOINT = "s3-fips.us-gov-east-1.amazonaws.com";

  private static final String VPC_ENDPOINT = "vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com";

  private static final String CN_VPC_ENDPOINT =
      "vpce-1a2b3c4d-5e6f.s3.cn-northwest-1.vpce.amazonaws.com.cn";


  private Configuration getConfiguration() {
    return new Configuration(false);
  }

  /**
   * Describe a test. This is a replacement for javadocs
   * where the tests role is printed in the log output
   * @param text description
   */
  protected void describe(String text) {
    LOG.info(text);
  }

  private RegionResolution.Resolution resolve(Configuration conf,
      String endpoint,
      String configuredRegion,
      boolean isFips,
      String expectedRegion,
      final RegionResolution.RegionResolutionMechanism expectedMechanism) throws IOException {
    S3ClientFactory.S3ClientCreationParameters parameters =
        new S3ClientFactory.S3ClientCreationParameters()
            .withEndpoint(endpoint)
            .withRegion(configuredRegion)
            .withFipsEnabled(isFips);
    final RegionResolution.Resolution resolved = calculateRegion(parameters, conf);

    // check the region
    if (expectedRegion != null) {
      Assertions.assertThat(resolved.getRegion())
          .describedAs("Resolved region %s", resolved)
          .isNotNull()
          .isEqualTo(Region.of(expectedRegion));
    } else {
      Assertions.assertThat(resolved.getRegion())
          .describedAs("Resolved region %s", resolved)
          .isNull();
    }

    // supplied resolution
    if (expectedMechanism != null) {
      assertMechanism(expectedMechanism, resolved);
    }
    return resolved;
  }

  /**
   * Assert that a resolution used a specific mechanism.
   * @param expectedMechanism expected mechanism.
   * @param resolved resolved region
   */
  private static void assertMechanism(
      final RegionResolution.RegionResolutionMechanism expectedMechanism,
      final RegionResolution.Resolution resolved) {
    Assertions.assertThat(resolved.getMechanism())
        .describedAs("Resolution mechanism of %s", resolved)
        .isEqualTo(expectedMechanism);
  }

  @Test
  public void testWithVPCE() throws IOException {
    resolve(getConfiguration(), VPC_ENDPOINT, null, false, US_WEST_2,
        RegionResolution.RegionResolutionMechanism.ParseVpceEndpoint);
  }

  @Test
  public void testWithChinaVPCE() throws IOException {
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), CN_VPC_ENDPOINT, null, false,
            CN_NORTHWEST_1, RegionResolution.RegionResolutionMechanism.ParseVpceEndpoint);
    assertEndpoint(r, CN_VPC_ENDPOINT);
    assertUseCentralValue(r, false);
  }

  @Test
  public void testCentralEndpointNoRegion() throws IOException {
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), CENTRAL_ENDPOINT, null, false,
            US_EAST_1,
            RegionResolution.RegionResolutionMechanism.FallbackToCentral);
    assertEndpoint(r, null);
    assertUseCentralValue(r, true);
  }

  @Test
  public void testCentralEndpointWithRegion() throws IOException {
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), CENTRAL_ENDPOINT, US_WEST_2, false,
            US_WEST_2, RegionResolution.RegionResolutionMechanism.Specified);
    assertEndpoint(r, null);
    assertUseCentralValue(r, true);
  }

  @Test
  public void testConfiguredRegion() throws IOException {
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), null, EU_WEST_2, false,
            EU_WEST_2, RegionResolution.RegionResolutionMechanism.Specified);
    // this still uses the central endpoint.
    assertEndpoint(r, null);
    assertUseCentralValue(r, true);
  }

  @Test
  public void testSDKRegion() throws IOException {
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), null, SDK_REGION, false,
            null, RegionResolution.RegionResolutionMechanism.Sdk);
    // SDK handles endpoint logic.
    assertEndpoint(r, null);
    assertUseCentralValue(r, true);
  }

  @Test
  public void testSDKUpperCaseRegion() throws IOException {
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), null, "SDK", false,
            null, RegionResolution.RegionResolutionMechanism.Sdk);
    // SDK handles endpoint logic.
    assertEndpoint(r, null);
    assertUseCentralValue(r, true);
  }

  @Test
  public void testEmptyStringRegion() throws IOException {
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), null, "", false,
            null, RegionResolution.RegionResolutionMechanism.Sdk);
    // SDK handles endpoint logic.
    assertEndpoint(r, null);
    assertUseCentralValue(r, true);
  }

  @Test
  public void testWithFipsNoEndpoint() throws IOException {
    describe("Create a client with fips enabled");

    resolve(getConfiguration(),
        null, EU_WEST_2, true,
        EU_WEST_2, RegionResolution.RegionResolutionMechanism.Specified);
  }

  /**
   * Attempting to create a client with fips enabled and an endpoint specified
   * fails during client construction.
   */
  @Test
  public void testWithFipsAndEndpoint() throws Exception {
    describe("Create a client with fips and an endpoint");

    intercept(IllegalArgumentException.class, ERROR_ENDPOINT_WITH_FIPS, () ->
        resolve(getConfiguration(), US_WEST_2, null, true, US_EAST_1, null));
  }

  @Test
  public void testWithRegionConfig() throws IOException {
    describe("Create a client with a configured region");

    resolve(getConfiguration(), null, EU_WEST_2, false,
        EU_WEST_2, RegionResolution.RegionResolutionMechanism.Specified);
  }

  @Test
  public void testEUWest2Endpoint() throws IOException {
    describe("specifying an eu-west-2 endpoint selects that region");

    resolve(getConfiguration(), EU_WEST_2_ENDPOINT, null, false,
        EU_WEST_2, RegionResolution.RegionResolutionMechanism.CalculatedFromEndpoint);
  }

  @Test
  public void testWithRegionAndEndpointConfig() throws IOException {
    describe("Test that when both region and endpoint are configured, region takes precedence");

    resolve(getConfiguration(), EU_WEST_2_ENDPOINT, US_WEST_2, false,
        US_WEST_2, RegionResolution.RegionResolutionMechanism.Specified);
  }

  @Test
  public void testWithChinaEndpoint() throws IOException {
    describe("Test with a china endpoint");
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), CN_ENDPOINT, null, false,
            CN_NORTHWEST_1,
            RegionResolution.RegionResolutionMechanism.CalculatedFromEndpoint);
    assertEndpoint(r, CN_ENDPOINT);
    assertUseCentralValue(r, false);
  }

  @Test
  public void testWithGovCloudEndpoint() throws IOException {
    describe("Test with a gov cloud endpoint");
    final RegionResolution.Resolution r =
        resolve(getConfiguration(), GOV_ENDPOINT, null, false,
            US_GOV_EAST_1,
            RegionResolution.RegionResolutionMechanism.CalculatedFromEndpoint);
    assertEndpoint(r, GOV_ENDPOINT);
    assertUseCentralValue(r, false);
  }

  @Test
  public void testNullIsForbidden() throws Throwable {
    describe("The region null is forbidden as a red flag of configuration problems");
    intercept(IllegalArgumentException.class, () ->
        resolve(getConfiguration(), null, "null", false,
            null, null));
  }

  @Test
  public void testGcsRegion() throws Throwable {
    resolve(getConfiguration(), "https://storage.googleapis.com", null, false,
        RegionResolution.EXTERNAL,
        RegionResolution.RegionResolutionMechanism.ExternalEndpoint);
  }

  @Test
  public void testLocalhostRegion() throws Throwable {
    resolve(getConfiguration(), "127.0.0.1", null, false,
        RegionResolution.EXTERNAL,
        RegionResolution.RegionResolutionMechanism.ExternalEndpoint);
  }

  /**
   * Assert that an endpoint matches the expected value.
   * @param r resolution
   * @param expected expected value.
   */
  private static void assertEndpoint(final RegionResolution.Resolution r,
      final String expected) {
    Assertions.assertThat(r.getEndpointStr())
        .describedAs("Endpoint of %s", r)
        .isEqualTo(expected);
  }

  /**
   * assert that the resolution {@code isUseCentralEndpoint()} value
   * matches that expected.
   * @param r resolution
   * @param expected expected value.
   */
  private static void assertUseCentralValue(final RegionResolution.Resolution r,
      final boolean expected) {
    Assertions.assertThat(r.isUseCentralEndpoint())
        .describedAs("Endpoint of %s", r)
        .isEqualTo(expected);
  }

}
