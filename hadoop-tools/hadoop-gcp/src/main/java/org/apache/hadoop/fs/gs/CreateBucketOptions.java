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

package org.apache.hadoop.fs.gs;

import java.time.Duration;

final class CreateBucketOptions {
  // TODO: Make sure the defaults have the setting matching the existing connector.
  static final CreateBucketOptions DEFAULT = new Builder().build();
  private final String location;
  private final String storageClass;
  private final Duration ttl;
  private final String projectId;

  private CreateBucketOptions(Builder builder) {
    this.location = builder.location;
    this.storageClass = builder.storageClass;
    this.ttl = builder.ttl;
    this.projectId = builder.projectId;
  }

  public String getLocation() {
    return location;
  }

  public String getStorageClass() {
    return storageClass;
  }

  public Duration getTtl() { // Changed return type to Duration
    return ttl;
  }

  static class Builder {
    private String location;
    private String storageClass;
    private Duration ttl;
    private String projectId;

    public Builder withLocation(String loc) {
      this.location = loc;
      return this;
    }

    public Builder withStorageClass(String sc) {
      this.storageClass = sc;
      return this;
    }

    public Builder withTtl(Duration ttlDuration) {
      this.ttl = ttlDuration;
      return this;
    }

    public Builder withProjectId(String pid) {
      this.projectId = pid;
      return this;
    }

    public CreateBucketOptions build() {
      return new CreateBucketOptions(this);
    }
  }
}

