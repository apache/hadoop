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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.util.AwsHostNameUtils;
import software.amazon.awssdk.regions.Region;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ClientFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CROSS_REGION_ACCESS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CROSS_REGION_ACCESS_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.EC2_REGION;
import static org.apache.hadoop.fs.s3a.Constants.EMPTY_REGION;
import static org.apache.hadoop.fs.s3a.Constants.FIPS_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SDK_REGION;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static software.amazon.awssdk.regions.Region.US_EAST_2;

/**
 * Region resolution.
 * <p>This is complicated and can be a source of support escalations.
 * <p>The V1 SDK was happy to take an endpoint and
 * work details out from there, possibly probing us-central-1 and cacheing
 * the result.
 * <p>The V2 SDK like the signing region and endpoint to be declared.
 * The S3A connector has tried to mimic the V1 code, but lacks some features
 * (use of environment variables, probing of EC2 IAM details) for which
 * the SDK is better.
 *
 */
public class RegionResolution {

  protected static final Logger LOG =
      LoggerFactory.getLogger(RegionResolution.class);

  /**
   * Service to ask SDK to parse.
   */
  private static final String S3_SERVICE_NAME = "s3";

  /**
   * Pattern to match vpce endpoints on.
   */
  private static final Pattern VPC_ENDPOINT_PATTERN =
      Pattern.compile("^(?:.+\\.)?([a-z0-9-]+)\\.vpce\\.amazonaws\\.(?:com|com\\.cn)$");

 /**
  * Error message when an endpoint is set with FIPS enabled: {@value}.
  */
 @VisibleForTesting
 public static final String ERROR_ENDPOINT_WITH_FIPS =
     "Only S3 central endpoint cannot be set when " + FIPS_ENDPOINT + " is true";

  /**
   * Virtual hostnames MUST be used when using the FIPS endpoint.
   */
 public static final String FIPS_PATH_ACCESS_INCOMPATIBLE =
     "Path style access must be disabled when "+ FIPS_ENDPOINT + " is true";

  /**
   * How was the region resolved?
   */
 public enum RegionResolutionMechanism {

   CalculatedFromEndpoint("Calculated from endpoint"),
   FallbackToCentral("Fallback to central endpoint"),
   ParseVpceEndpoint("Parse VPCE Endpoint"),
   Sdk("SDK resolution chain"),
   Specified("region specified");

   /**
    * Text of the mechanism.
    */
   private final String mechanism;

   RegionResolutionMechanism(String mechanism) {
     this.mechanism = mechanism;
   }

    /**
     * String value of the resolution mechanism.
     * @return the resolution mechanism.
     */
   public String getMechanism() {
     return mechanism;
   }

   @Override
   public String toString() {
     final StringBuilder sb = new StringBuilder("RegionResolutionMechanism{");
     sb.append("mechanism='").append(mechanism).append('\'');
     sb.append('}');
     return sb.toString();
   }
 }

  /**
   * The resolution of a region and endpoint..
   */
  public static final class Resolution {

    /**
     * Region: if null hand down to the SDK.
     */
    private Region region;

    /**
     * How was the region resolved?
     * Null means unresolved.
     */
    private RegionResolutionMechanism resolution;

    /**
     * Should FIPS be enabled?
     */
    private boolean useFips;

    /**
     * Should cross-region access be enabled?
     */
    private boolean crossRegionAccessEnabled;

    /**
     * Endpoint as string.
     */
    private String endpointStr;

    /**
     * Endpoint URI.
     */
    private URI endpointUri;

    /**
     * Use the central endpoint?
     */
    private boolean useCentralEndpoint;

    /**
     * Set the region.
     * Declares the region as resolved even when the value is null (i.e. resolve to SDK).
     * @param region new value
     * @return the builder
     */
    public Resolution withRegion(
        @Nullable final Region region,
        final RegionResolutionMechanism resolutionMechanism) {
      this.region = region;
      this.resolution = requireNonNull(resolutionMechanism);
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public Resolution withUseFips(final boolean value) {
      useFips = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public Resolution withCrossRegionAccessEnabled(final boolean value) {
      crossRegionAccessEnabled = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public Resolution withEndpointStr(final String value) {
      endpointStr = value;
      return this;
    }

    public URI getEndpointUri() {
      return endpointUri;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public Resolution withEndpointUri(final URI value) {
      endpointUri = value;
      return this;
    }

    public Region getRegion() {
      return region;
    }

    public boolean isUseFips() {
      return useFips;
    }

    public boolean isCrossRegionAccessEnabled() {
      return crossRegionAccessEnabled;
    }

    public RegionResolutionMechanism getResolution() {
      return resolution;
    }

    public String getEndpointStr() {
      return endpointStr;
    }

    public boolean isRegionResolved() {
      return resolution != null;
    }

    public boolean isUseCentralEndpoint() {
      return useCentralEndpoint;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public Resolution withUseCentralEndpoint(final boolean value) {
      useCentralEndpoint = value;
      return this;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Resolution{");
      sb.append("region=").append(region);
      sb.append(", resolution=").append(resolution);
      sb.append(", useFips=").append(useFips);
      sb.append(", crossRegionAccessEnabled=").append(crossRegionAccessEnabled);
      sb.append(", endpointUri=").append(endpointUri);
      sb.append(", useCentralEndpoint=").append(useCentralEndpoint);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Given a endpoint string, create the endpoint URI.
   *
   * @param endpoint possibly null endpoint.
   * @param secureConnections use secure HTTPS connection?
   * @return an endpoint uri or null if the endpoint was passed in was null/empty
   * @throws IllegalArgumentException failure to parse the endpoint.
   */
  public static URI buildEndpointUri(String endpoint, final boolean secureConnections) {

    String protocol = secureConnections ? "https" : "http";

    if (endpoint == null || endpoint.isEmpty()) {
      // don't set an endpoint if none is configured, instead let the SDK figure it out.
      return null;
    }

    if (!endpoint.contains("://")) {
      endpoint = String.format("%s://%s", protocol, endpoint);
    }

    try {
      return new URI(endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Parses the endpoint to get the region.
   * If endpoint is the central one, use US_EAST_2.
   * @param endpoint the configure endpoint.
   * @param endpointEndsWithCentral true if the endpoint is configured as central.
   * @return the S3 region resolution if possible from parsing the endpoint
   */
  @VisibleForTesting
  public static Optional<Resolution> getS3RegionFromEndpoint(
      final String endpoint,
      final boolean endpointEndsWithCentral) {

    if (!endpointEndsWithCentral) {
      // S3 VPC endpoint parsing
      Matcher matcher = VPC_ENDPOINT_PATTERN.matcher(endpoint);
      if (matcher.find()) {
        LOG.debug("Mapping to VPCE");
        LOG.debug("Endpoint {} is vpc endpoint; parsing region as {}", endpoint, matcher.group(1));
        return Optional.of(new Resolution()
            .withRegion(Region.of(matcher.group(1)),
                RegionResolutionMechanism.ParseVpceEndpoint));
      }

      LOG.debug("Endpoint {} is not the default; parsing", endpoint);
      return AwsHostNameUtils.parseSigningRegion(endpoint, S3_SERVICE_NAME)
          .map(r ->
              new Resolution().withRegion(r,
                  RegionResolutionMechanism.CalculatedFromEndpoint));
    }

    // No resolution.
    return Optional.empty();
  }

  /**
   * Calculate the region and the final endpoint.
   * @param parameters creation parameters
   * @param conf configuration with other options.
   * @return the resolved region and endpoint.
   * @throws IllegalArgumentException failure to parse endpoint, or FIPS settings.
   */
  public static Resolution calculateRegion(
      final S3ClientFactory.S3ClientCreationParameters parameters,
      final Configuration conf) {

    final Resolution resolution = new Resolution();

    // endpoint; may be null
    final String endpointStr = parameters.getEndpoint();
    // will be null if endpointStr is null/empty
    final URI endpoint = buildEndpointUri(endpointStr,
        conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS));

    final String configuredRegion = parameters.getRegion();

    // If the region was configured, set it.
    // this includes special handling of the sdk, ec2 and "" regions.
    if (configuredRegion != null) {
      switch (configuredRegion.toLowerCase(Locale.ROOT)) {
      case EC2_REGION:
      case SDK_REGION:
      case EMPTY_REGION:
        resolution.withRegion(null, RegionResolutionMechanism.Sdk);
        break;

      default:
        resolution.withRegion(Region.of(configuredRegion),
            RegionResolutionMechanism.Specified);
      }
    }


    // cross region setting.
    resolution.withCrossRegionAccessEnabled(
        conf.getBoolean(AWS_S3_CROSS_REGION_ACCESS_ENABLED,
            AWS_S3_CROSS_REGION_ACCESS_ENABLED_DEFAULT));

    // central endpoint if no endpoint has been set, or it is explicitly
    // requested
    boolean endpointEndsWithCentral = endpointStr == null
        || endpointStr.isEmpty()
        || endpointStr.endsWith(CENTRAL_ENDPOINT);

    // fips settings.
    final boolean fipsEnabled = parameters.isFipsEnabled();
    resolution.withUseFips(fipsEnabled);
    if (fipsEnabled) {
      // validate the FIPS settings
      checkArgument(endpoint == null || endpointEndsWithCentral,
          "%s : %s", ERROR_ENDPOINT_WITH_FIPS, endpoint);
      checkArgument(!parameters.isPathStyleAccess(),
          FIPS_PATH_ACCESS_INCOMPATIBLE);
    }

    if (!resolution.isRegionResolved()) {
      // parse from the endpoint and set if calculated
      LOG.debug("Falling back to parsing region endpoint {}; endpointEndsWithCentral={}",
          endpointStr, endpointEndsWithCentral);
      final Optional<Resolution> regionFromEndpoint =
          getS3RegionFromEndpoint(endpointStr, endpointEndsWithCentral);
      if (regionFromEndpoint.isPresent()) {
        regionFromEndpoint
            .map(r ->
                resolution.withRegion(r.getRegion(), r.getResolution()));
      }
    }
    if (!resolution.isRegionResolved()) {
      // still failing to resolve the region
      // fall back to central
      resolution.withRegion(US_EAST_2, RegionResolutionMechanism.FallbackToCentral);
    }

    // No need to override endpoint with "s3.amazonaws.com".
    // Let the client take care of endpoint resolution. Overriding
    // the endpoint with "s3.amazonaws.com" causes 400 Bad Request
    // errors for non-existent buckets and objects.
    // ref: https://github.com/aws/aws-sdk-java-v2/issues/4846
    if (!endpointEndsWithCentral) {
      LOG.debug("Setting endpoint to {}", endpoint);
      resolution.withEndpointStr(endpointStr)
          .withEndpointUri(endpoint)
          .withUseCentralEndpoint(false);
    } else {
      resolution.withUseCentralEndpoint(true);
    }

    final Region r = resolution.getRegion();
    if (r != null && Region.regions().contains(r)) {
      // note that the region isn't known.
      // not an issue for third party stores, otherwise it may be a region newer than
      // that expected by the SDK. Hence: only log at debug.
      LOG.debug("Region {} is not recognized by this SDK", r);
    }
    return resolution;
  }

}
