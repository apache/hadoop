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

import com.amazonaws.arn.Arn;
import com.amazonaws.regions.RegionUtils;

/**
 * Represents an Arn Resource, this can be an accesspoint or bucket.
 */
public class ArnResource {

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
   * Because of the different ways an endpoint can be constructed depending on partition we're
   * relying on the AWS SDK to produce the endpoint. In this case we need a region key of the form
   * {@code String.format("accesspoint-%s", awsRegion)}
   */
  private final String accessPointRegionKey;

  private ArnResource(String name, String owner, String region, String partition, String fullArn) {
    this.name = name;
    this.ownerAccountId = owner;
    this.region = region;
    this.partition = partition;
    this.fullArn = fullArn;
    this.accessPointRegionKey = String.format("accesspoint-%s", region);
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
   * Formatted endpoint for the resource.
   * @return resource endpoint.
   */
  public String getEndpoint() {
    return RegionUtils.getRegion(accessPointRegionKey)
        .getServiceEndpoint("s3");
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

    if (parsed.getRegion().isEmpty() || parsed.getAccountId().isEmpty() ||
        parsed.getResourceAsString().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Access Point Arn %s has an invalid format or missing properties", arn));
    }

    String resourceName = parsed.getResource().getResource();
    return new ArnResource(resourceName, parsed.getAccountId(), parsed.getRegion(),
        parsed.getPartition(), arn);
  }
}
