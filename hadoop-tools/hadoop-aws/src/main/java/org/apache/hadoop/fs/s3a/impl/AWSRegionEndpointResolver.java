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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.util.AwsHostNameUtils;
import software.amazon.awssdk.regions.Region;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.store.LogExactlyOnce;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CROSS_REGION_ACCESS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CROSS_REGION_ACCESS_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_DEFAULT_REGION;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.FIPS_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * This class uses Hadoop configurations to resolve endpoint and region information which
 * is then set in the SDK clients.
 */
public class AWSRegionEndpointResolver {

  private static final String S3_SERVICE_NAME = "s3";

  private static final Pattern VPC_ENDPOINT_PATTERN =
      Pattern.compile("^(?:.+\\.)?([a-z0-9-]+)\\.vpce\\.amazonaws\\.(?:com|com\\.cn)$");

  protected static final Logger LOG =
      LoggerFactory.getLogger(AWSRegionEndpointResolver.class);

  /**
   * A one-off warning of default region chains in use.
   */
  private static final LogExactlyOnce WARN_OF_DEFAULT_REGION_CHAIN =
      new LogExactlyOnce(LOG);

  /**
   * Warning message printed when the SDK Region chain is in use.
   */
  private static final String SDK_REGION_CHAIN_IN_USE =
      "S3A filesystem client is using"
          + " the SDK region resolution chain.";

  /**
   * Error message when an endpoint is set with FIPS enabled: {@value}.
   */
  @VisibleForTesting
  public static final String ERROR_ENDPOINT_WITH_FIPS =
      "Non central endpoint cannot be set when " + FIPS_ENDPOINT + " is true";

  private AWSRegionEndpointResolver() {}

  public static AWSRegionEndpointInformation getEndpointRegionResolution(
          S3ClientFactory.S3ClientCreationParameters parameters, Configuration conf) {
    final String endpointStr = parameters.getEndpoint();
    final URI endpoint = getS3Endpoint(endpointStr, conf);

    AWSRegionEndpointInformation.Builder builder = new AWSRegionEndpointInformation.Builder();

    final String configuredRegion = parameters.getRegion();
    Region region = null;
    String origin = "";

    // If the region was configured, set it.
    if (configuredRegion != null && !configuredRegion.isEmpty()) {
      origin = AWS_REGION;
      region = Region.of(configuredRegion);
    }

    // FIPs? Log it, then reject any attempt to set an endpoint
    final boolean fipsEnabled = parameters.isFipsEnabled();
    if (fipsEnabled) {
      LOG.debug("Enabling FIPS mode");
    }
    // always setting it guarantees the value is non-null,
    // which tests expect.
    builder.fipsEnabled(fipsEnabled);

    if (endpoint != null) {
      boolean endpointEndsWithCentral =
          endpointStr.endsWith(CENTRAL_ENDPOINT);
      checkArgument(!fipsEnabled || endpointEndsWithCentral, "%s : %s",
          ERROR_ENDPOINT_WITH_FIPS,
          endpoint);

      // No region was configured,
      // determine the region from the endpoint.
      if (region == null) {
        region = getS3RegionFromEndpoint(endpointStr,
            endpointEndsWithCentral);
        if (region != null) {
          origin = "endpoint";
        }
      }

      // No need to override endpoint with "s3.amazonaws.com".
      // Let the client take care of endpoint resolution. Overriding
      // the endpoint with "s3.amazonaws.com" causes 400 Bad Request
      // errors for non-existent buckets and objects.
      // ref: https://github.com/aws/aws-sdk-java-v2/issues/4846
      if (!endpointEndsWithCentral) {
        builder.withEndpoint(endpoint);
        LOG.debug("Setting endpoint to {}", endpoint);
      } else {
        origin = "central endpoint with cross region access";
        LOG.debug("Enabling cross region access for endpoint {}",
            endpointStr);
      }
    }

    if (region != null) {
      builder.withRegion(region);
    } else if (configuredRegion == null) {
      // no region is configured, and none could be determined from the endpoint.
      // Use US_EAST_2 as default.
      region = Region.of(AWS_S3_DEFAULT_REGION);
      builder.withRegion(region);
      origin = "cross region access fallback";
    } else if (configuredRegion.isEmpty()) {
      // region configuration was set to empty string.
      // allow this if people really want it; it is OK to rely on this
      // when deployed in EC2.
      LOG.debug(SDK_REGION_CHAIN_IN_USE);
      origin = "SDK region chain";
    }
    boolean isCrossRegionAccessEnabled = conf.getBoolean(AWS_S3_CROSS_REGION_ACCESS_ENABLED,
        AWS_S3_CROSS_REGION_ACCESS_ENABLED_DEFAULT);
    // s3 cross region access
    if (isCrossRegionAccessEnabled) {
      builder.crossRegionAccessEnabled(true);
    }
    LOG.debug("Setting region to {} from {} with cross region access {}",
        region, origin, isCrossRegionAccessEnabled);

    return builder.build();
  }

  /**
   * Builds a URI from the configured S3 endpoint.
   * Will throw an IllegalArgumentException for malformed endpoint strings, for example:
   * https ://s3.us-east-1.amazonaws.com
   *
   * @param endpoint configured S3 endpoint
   * @param conf configuration
   * @return S3 endpoint URI.
   */
  protected static URI getS3Endpoint(String endpoint, final Configuration conf) {

    boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);

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
   *
   * @param endpoint the configure endpoint.
   * @param endpointEndsWithCentral true if the endpoint is configured as central.
   * @return the S3 region, null if unable to resolve from endpoint.
   */
  @VisibleForTesting
  public static Region getS3RegionFromEndpoint(final String endpoint,
      final boolean endpointEndsWithCentral) {

    if (!endpointEndsWithCentral) {
      // S3 VPC endpoint parsing
      Matcher matcher = VPC_ENDPOINT_PATTERN.matcher(endpoint);
      if (matcher.find()) {
        LOG.debug("Mapping to VPCE");
        LOG.debug("Endpoint {} is vpc endpoint; parsing region as {}", endpoint, matcher.group(1));
        return Region.of(matcher.group(1));
      }

      LOG.debug("Endpoint {} is not the default; parsing", endpoint);
      return AwsHostNameUtils.parseSigningRegion(endpoint, S3_SERVICE_NAME).orElse(null);
    }

    // Select default region here to enable cross-region access.
    // If both "fs.s3a.endpoint" and "fs.s3a.endpoint.region" are empty,
    // Spark sets "fs.s3a.endpoint" to "s3.amazonaws.com".
    // This applies to Spark versions with the changes of SPARK-35878.
    // ref:
    // https://github.com/apache/spark/blob/v3.5.0/core/
    // src/main/scala/org/apache/spark/deploy/SparkHadoopUtil.scala#L528
    // If we do not allow cross region access, Spark would not be able to
    // access any bucket that is not present in the given region.
    // Hence, we should use default region us-east-2 to allow cross-region
    // access.
    return Region.of(AWS_S3_DEFAULT_REGION);
  }



}
