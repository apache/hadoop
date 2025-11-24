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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/** Options that can be specified when creating a file in the {@link GoogleCloudStorage}. */

final class CreateObjectOptions {
  static final CreateObjectOptions DEFAULT_OVERWRITE = builder().setOverwriteExisting(true).build();

  private final String contentEncoding;
  private final String contentType;
  private final boolean ensureEmptyObjectsMetadataMatch;
  private final String kmsKeyName;
  private final ImmutableMap<String, byte[]> metadata;
  private final boolean overwriteExisting;

  private CreateObjectOptions(Builder builder) {
    this.contentEncoding = builder.contentEncoding;
    this.contentType = builder.contentType;
    this.ensureEmptyObjectsMetadataMatch = builder.ensureEmptyObjectsMetadataMatch;
    this.kmsKeyName = builder.kmsKeyName;
    this.metadata = ImmutableMap.copyOf(builder.metadata);
    this.overwriteExisting = builder.overwriteExisting;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getContentEncoding() {
    return contentEncoding;
  }

  public String getContentType() {
    return contentType;
  }

  public boolean isEnsureEmptyObjectsMetadataMatch() {
    return ensureEmptyObjectsMetadataMatch;
  }

  public String getKmsKeyName() {
    return kmsKeyName;
  }

  public Map<String, byte[]> getMetadata() {
    return metadata;
  }

  public boolean isOverwriteExisting() {
    return overwriteExisting;
  }

  public Builder toBuilder() {
    return builder().setContentEncoding(this.contentEncoding).setContentType(this.contentType)
        .setEnsureEmptyObjectsMetadataMatch(this.ensureEmptyObjectsMetadataMatch)
        .setKmsKeyName(this.kmsKeyName).setMetadata(this.metadata)
        .setOverwriteExisting(this.overwriteExisting);
  }

  static final class Builder {
    private String contentEncoding;
    private String contentType;
    private boolean ensureEmptyObjectsMetadataMatch = false;
    private String kmsKeyName;
    private Map<String, byte[]> metadata = new HashMap<>();
    private boolean overwriteExisting = false;

    private Builder() {
    }

    public Builder setContentEncoding(String ce) {
      this.contentEncoding = ce;
      return this;
    }

    public Builder setContentType(String ct) {
      this.contentType = ct;
      return this;
    }

    public Builder setEnsureEmptyObjectsMetadataMatch(boolean val) {
      this.ensureEmptyObjectsMetadataMatch = val;
      return this;
    }

    public Builder setKmsKeyName(String key) {
      this.kmsKeyName = key;
      return this;
    }

    public Builder setMetadata(Map<String, byte[]> m) {
      this.metadata = m;
      return this;
    }

    public Builder setOverwriteExisting(boolean overwrite) {
      this.overwriteExisting = overwrite;
      return this;
    }

    public CreateObjectOptions build() {
      return new CreateObjectOptions(this);
    }
  }
}
