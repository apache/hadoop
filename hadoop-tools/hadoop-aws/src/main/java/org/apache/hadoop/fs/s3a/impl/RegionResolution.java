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
import software.amazon.awssdk.regions.providers.InstanceProfileRegionProvider;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
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
import static org.apache.hadoop.fs.s3a.impl.RegionResolution.RegionResolutionMechanism.ExternalEndpoint;
import static org.apache.hadoop.fs.s3a.impl.RegionResolution.RegionResolutionMechanism.FallbackToCentral;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

/**
 * Region resolution.
 * <p>This is complicated and can be a source of support escalations.
 * <p>The V1 SDK was happy to take an endpoint and
 * work details out from there, possibly probing us-central-1 and cacheing
 * the result.
 * <p>The V2 SDK likes the signing region and endpoint to be declared.
 * The S3A connector has tried to mimic the V1 code, but lacks some features
 * (use of environment variables, probing of EC2 IAM details) for which
 * the SDK is better.
 * <ol>
 * <li>If region is configured via fs.s3a.endpoint.region, use it.</li>
 * <li>If endpoint is configured via via fs.s3a.endpoint, set it.
 *     If no region is configured, try to parse region from endpoint. </li>
 * <li> If no region is configured, and it could not be parsed from the endpoint,
 *     set the default region as US_EAST_2</li>
 * <li> If configured region is empty, fallback to SDK resolution chain. </li>
 * <li> S3 cross region is enabled by default irrespective of region or endpoint
 *      is set or not.</li>
 * </ol>
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
      "Path style access must be disabled when " + FIPS_ENDPOINT + " is true";

  /**
   * String value for external region: {@value}.
   */
  public static final String EXTERNAL = "external";

  /**
   * External region, used for third party endpoints.
   */
  public static final Region EXTERNAL_REGION = Region.of(EXTERNAL);

  /**
   * How was the region resolved?
   */
  public enum RegionResolutionMechanism {

    CalculatedFromEndpoint("Calculated from endpoint"),
    ExternalEndpoint("External endpoint"),
    FallbackToCentral("Fallback to central endpoint"),
    ParseVpceEndpoint("Parse VPCE Endpoint"),
    Ec2Metadata("EC2 Metadata"),
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
    private RegionResolutionMechanism mechanism;

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

    public Resolution() {
    }

    /**
     * Instantiate with a region and resolution mechanism.
     * @param region region
     * @param mechanism resolution mechanism.
     */
    public Resolution(final Region region, final RegionResolutionMechanism mechanism) {
      this.region = region;
      this.mechanism = mechanism;
    }

    /**
     * Set the region.
     * Declares the region as resolved even when the value is null (i.e. resolve to SDK).
     * @param region region
     * @param resolutionMechanism resolution mechanism.
     * @return the builder
     */
    public Resolution withRegion(
        @Nullable final Region region,
        final RegionResolutionMechanism resolutionMechanism) {
      this.region = region;
      this.mechanism = requireNonNull(resolutionMechanism);
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

    public RegionResolutionMechanism getMechanism() {
      return mechanism;
    }

    public String getEndpointStr() {
      return endpointStr;
    }

    public boolean isRegionResolved() {
      return mechanism != null;
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
      sb.append(", resolution=").append(mechanism);
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
   * If endpoint is the central one, use US_EAST_1.
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
        LOG.debug("Endpoint {} is VPC endpoint; parsing region as {}",
            endpoint, matcher.group(1));
        return Optional.of(new Resolution(
            Region.of(matcher.group(1)),
            RegionResolutionMechanism.ParseVpceEndpoint));
      }

      LOG.debug("Endpoint {} is not the default; parsing", endpoint);
      return AwsHostNameUtils.parseSigningRegion(endpoint, S3_SERVICE_NAME)
          .map(r ->
              new Resolution(r, RegionResolutionMechanism.CalculatedFromEndpoint));
    }

    // No resolution.
    return Optional.empty();
  }

  /**
   * Is this an AWS endpoint, that is: has an endpoint been set which matches
   * amazon.
   * @param endpoint non-null endpoint URL
   * @return true if this is amazonaws or amazonaws china
   */
  public static boolean isAwsEndpoint(final String endpoint) {
    final String h = endpoint.toLowerCase(Locale.ROOT);
    // Common AWS partitions: global (.amazonaws.com) and China (.amazonaws.com.cn).
    return h.endsWith(".amazonaws.com")
        || h.endsWith(".amazonaws.com.cn");
  }


  /**
   * Does the region name refer to an SDK region?
   * @param configuredRegion region in the configuration
   * @return true if this is considered to refer to an SDK region.
   */
  public static boolean isSdkRegion(String configuredRegion) {
    return SDK_REGION.equalsIgnoreCase(configuredRegion)
        || EMPTY_REGION.equalsIgnoreCase(configuredRegion);
  }

  /**
   * Does the region name refer to {@code "ec2"} in which case special handling
   * is required.
   * @param configuredRegion region in the configuration
   * @return true if this is considered to refer to an SDK region.
   */
  public static boolean isEc2Region(String configuredRegion) {
    return EC2_REGION.equalsIgnoreCase(configuredRegion);
  }

  /**
   * Calculate the region and the final endpoint.
   * @param parameters creation parameters
   * @param conf configuration with other options.
   * @return the resolved region and endpoint.
   * @throws IOException if the client failed to communicate with the IAM service.
   * @throws IllegalArgumentException failure to parse endpoint, or FIPS settings.
   */
  @Retries.OnceTranslated
  public static Resolution calculateRegion(
      final S3ClientFactory.S3ClientCreationParameters parameters,
      final Configuration conf) throws IOException {

    Resolution resolution = new Resolution();

    // endpoint; may be null
    final String endpointStr = parameters.getEndpoint();
    boolean endpointDeclared = endpointStr != null && !endpointStr.isEmpty();
    // will be null if endpointStr is null/empty
    final URI endpoint = buildEndpointUri(endpointStr,
        conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS));

    final String configuredRegion = parameters.getRegion();

    // If the region was configured, set it.
    // this includes special handling of the sdk, ec2 and "" regions.
    if (configuredRegion != null) {
      checkArgument(!"null".equals(configuredRegion),
          "null is region name");
      if (isSdkRegion(configuredRegion)) {
        resolution.withRegion(null, RegionResolutionMechanism.Sdk);
      } else if (isEc2Region(configuredRegion)) {
        // special EC2 handling
        final Resolution r = getS3RegionFromEc2IAM();
        resolution.withRegion(r.getRegion(), r.getMechanism());
      } else {
        resolution.withRegion(Region.of(configuredRegion),
            RegionResolutionMechanism.Specified);
      }
    }

    // central endpoint if no endpoint has been set, or it is explicitly
    // requested
    boolean endpointEndsWithCentral = !endpointDeclared
        || endpointStr.endsWith(CENTRAL_ENDPOINT);

    if (!resolution.isRegionResolved()) {
      // parse from the endpoint and set if calculated
      LOG.debug("Falling back to parsing region endpoint {}; endpointEndsWithCentral={}",
          endpointStr, endpointEndsWithCentral);
      final Optional<Resolution> regionFromEndpoint =
          getS3RegionFromEndpoint(endpointStr, endpointEndsWithCentral);
      if (regionFromEndpoint.isPresent()) {
        regionFromEndpoint
            .map(r ->
                resolution.withRegion(r.getRegion(), r.getMechanism()));
      }
    }

    // cross region setting.
    resolution.withCrossRegionAccessEnabled(
        conf.getBoolean(AWS_S3_CROSS_REGION_ACCESS_ENABLED,
            AWS_S3_CROSS_REGION_ACCESS_ENABLED_DEFAULT));

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
      // still not resolved.
      if (!endpointDeclared || isAwsEndpoint(endpointStr)) {
        // still failing to resolve the region
        // fall back to central
        resolution.withRegion(US_EAST_1, FallbackToCentral);
      } else {
        // we are not resolved and not an aws region.
        // set the region to being "external"
        resolution.withRegion(EXTERNAL_REGION, ExternalEndpoint);
      }
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

  /**
   * Probes EC2 Metadata for the region.
   * This uses a class {@code InstanceProfileRegionProvider} which AWS
   * declare as for internal use only.
   * Linking/invocation should be caught and downgraded to returning an empty() option.
   * @return the region from EC2 IAM.
   * @throws IOException if the client failed to communicate with the IAM service.
   */
  @VisibleForTesting
  @Retries.OnceTranslated
  static Resolution getS3RegionFromEc2IAM() throws IOException {
    return Invoker.once("Resolve EC2 Metadata", "/", () -> {
      LOG.debug("Resolving region through EC2 Metadata");
      final Region region = new InstanceProfileRegionProvider().getRegion();
      return new Resolution(region, RegionResolutionMechanism.Ec2Metadata);
    });
  }
}
