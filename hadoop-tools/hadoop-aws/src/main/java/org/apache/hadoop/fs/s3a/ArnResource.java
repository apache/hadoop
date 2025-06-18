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

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nonnull;

import software.amazon.awssdk.arns.Arn;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents an Arn Resource, this can be an accesspoint or bucket.
 */
public final class ArnResource {
  private final static String S3_ACCESSPOINT_ENDPOINT_FORMAT = "s3-accesspoint.%s.amazonaws.com";
  private final static String S3_OUTPOSTS_ACCESSPOINT_ENDPOINT_FORMAT = "s3-outposts.%s.amazonaws.com";
  private final static String S3_EXPRESS_ACCESSPOINT_ENDPOINT_FORMAT = "s3express-%s.%s.amazonaws.com";
  // bucket example: mybucket--usw2-az1--x-s3
  // access point example: myaccesspoint--usw2-az1--xa-s3
  public final static Pattern S3_EXPRESS_RESOURCE_FORMAT_REGEX = Pattern.compile(
    String.format("^(?<apname>[a-z0-9]([a-z0-9\\-]*[a-z0-9])?)--(?<zoneId>[a-z0-9\\-]+)--(?<resource>x|xa)-s3$")
  );

  /**
   * Resource name.
   */
  private final String name;

  /**
   * Resource owner account id.
   */
  private final String ownerAccountId;

  /**
   * Resource region.
   */
  private final String region;

  /**
   * Full Arn for the resource.
   */
  private final String fullArn;

  /**
   * Partition for the resource. Allowed partitions: aws, aws-cn, aws-us-gov
   */
  private final String partition;

  /**
   * Service for the resource. Allowed services: s3, s3-outposts, s3express
   */
  private final String service;

  /**
   * Because of the different ways an endpoint can be constructed depending on partition we're
   * relying on the AWS SDK to produce the endpoint. In this case we need a region key of the form
   * {@code String.format("accesspoint-%s", awsRegion)}
   */
  private final String accessPointRegionKey;

  private ArnResource(String name, String owner, String region, String partition, String fullArn, String service) {
    this.name = name;
    this.ownerAccountId = owner;
    this.region = region;
    this.partition = partition;
    this.fullArn = fullArn;
    this.service = service;
    this.accessPointRegionKey = String.format("accesspoint-%s", region);
  }

  private boolean isOutposts(){
    return fullArn.contains("s3-outposts");
  }

  private boolean isExpress(){
    return fullArn.contains("s3express");
  }

  /**
   * Resource name.
   * @return resource name.
   */
  public String getName() {
    return name;
  }

  /**
   * Return owner's account id.
   * @return owner account id
   */
  public String getOwnerAccountId() {
    return ownerAccountId;
  }

  /**
   * Resource region.
   * @return resource region.
   */
  public String getRegion() {
    return region;
  }

  /**
   * Full arn for resource.
   * @return arn for resource.
   */
  public String getFullArn() {
    return fullArn;
  }

  /**
   * Service for resource.
   * @return service for resource.
   */
  public String getService() {
    return service;
  }

  /**
   * Formatted endpoint for the resource.
   * @return resource endpoint.
   */
  public String getEndpoint() {
    String format;
    if(isExpress()) {
      Optional<String> zoneId = getZoneIdFromResourceName(name);
      if(zoneId.isEmpty()) {
        throw new IllegalArgumentException("Zone ID could not be extracted from S3Express resource name: " + name);
      }

      format = S3_EXPRESS_ACCESSPOINT_ENDPOINT_FORMAT;
      return String.format(format, zoneId.get(), region);
    } else if (isOutposts()) {
      format = S3_OUTPOSTS_ACCESSPOINT_ENDPOINT_FORMAT;
      return String.format(format, region);
    } else {
      format = S3_ACCESSPOINT_ENDPOINT_FORMAT;
      return String.format(format, region);
    }
  }

  /**
   * Parses the passed `arn` string into a full ArnResource.
   * @param arn - string representing an Arn resource.
   * @return new ArnResource instance.
   * @throws IllegalArgumentException - if the Arn is malformed or any of the region, accountId and
   * resource name properties are empty.
   */
  @Nonnull
  public static ArnResource accessPointFromArn(String arn) throws IllegalArgumentException {
    Arn parsed = Arn.fromString(arn);

    if (!parsed.region().isPresent() || !parsed.accountId().isPresent() ||
        parsed.resourceAsString().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Access Point Arn %s has an invalid format or missing properties", arn));
    }

    String resourceName = parsed.resource().resource();
    return new ArnResource(resourceName, parsed.accountId().get(), parsed.region().get(),
        parsed.partition(), arn, parsed.service());
  }

  private static Optional<String> getZoneIdFromResourceName(final String resourceName) {   
    return Optional.ofNullable(resourceName)
        .map(name -> {
            Matcher matcher = S3_EXPRESS_RESOURCE_FORMAT_REGEX.matcher(name);
            return matcher.matches() ? matcher.group("zoneId") : null;
        });
  }
}
