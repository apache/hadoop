/**
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

package org.apache.hadoop.runc.docker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestRefV2 {

  public static final String CONTENT_TYPE
      = "application/vnd.docker.distribution.manifest.v2+json";

  private String digest;
  private String mediaType;
  private PlatformV2 platform;
  private long size;

  @JsonProperty
  public String getDigest() {
    return digest;
  }

  public void setDigest(String digest) {
    this.digest = digest;
  }

  @JsonProperty
  public String getMediaType() {
    return mediaType;
  }

  public void setMediaType(String mediaType) {
    this.mediaType = mediaType;
  }

  @JsonProperty
  public PlatformV2 getPlatform() {
    return platform;
  }

  public void setPlatform(PlatformV2 platform) {
    this.platform = platform;
  }

  @JsonProperty
  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  @Override public String toString() {
    return String.format(
        "{ digest=%s, mediaType=%s, platform=%s, size=%d }",
        digest, mediaType, platform, size);
  }

}
