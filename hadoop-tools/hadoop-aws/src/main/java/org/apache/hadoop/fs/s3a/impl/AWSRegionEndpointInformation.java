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

import software.amazon.awssdk.regions.Region;

/**
 * Container class to store endpoint and region information which is used to configure SDK
 * clients.
 */
public class AWSRegionEndpointInformation {

  private final URI endpoint;
  private final Region region;
  private final boolean fipsEnabled;
  private final boolean crossRegionAccessEnabled;


  AWSRegionEndpointInformation(Builder builder) {
    this.endpoint = builder.endpoint;
    this.region = builder.region;
    this.fipsEnabled = builder.fipsEnabled;
    this.crossRegionAccessEnabled = builder.crossRegionAccessEnabled;
  }

  public URI getEndpoint() {
    return endpoint;
  }

  public Region getRegion() {
    return region;
  }

  public boolean isFipsEnabled() {
    return fipsEnabled;
  }

  public boolean isCrossRegionAccessEnabled() {
    return crossRegionAccessEnabled;
  }

  public static class Builder {

    private URI endpoint;
    private Region region;
    private boolean fipsEnabled;
    private boolean crossRegionAccessEnabled;


    public AWSRegionEndpointInformation build() {
      return new AWSRegionEndpointInformation(this);
    }

    public Builder withRegion(final Region value) {
      this.region = value;
      return this;
    }

    public Builder withEndpoint(final URI value) {
      this.endpoint = value;
      return this;
    }

    public Builder fipsEnabled(final boolean value) {
      this.fipsEnabled = value;
      return this;
    }

    public Builder crossRegionAccessEnabled(final boolean value) {
      this.crossRegionAccessEnabled = value;
      return this;
    }
  }
}
