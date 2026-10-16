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

import javax.annotation.Nullable;

import static org.apache.hadoop.fs.gs.Constants.PATH_DELIMITER;

/** Options that can be specified when listing objects in the {@link GoogleCloudStorage}. */
final class ListObjectOptions {

  /** List all objects in the directory. */
  public static final ListObjectOptions DEFAULT = new Builder().build();

  /** List all objects with the prefix. */
  public static final ListObjectOptions DEFAULT_FLAT_LIST =
      DEFAULT.builder().setDelimiter(null).build();

  Builder builder() {
    Builder result = new Builder();
    result.fields = fields;
    result.delimiter = delimiter;
    result.maxResults = maxResult;
    result.includePrefix = includePrefix;

    return result;
  }

  private final String delimiter;
  private final boolean includePrefix;
  private final long maxResult;
  private final String fields;

  private ListObjectOptions(Builder builder) {
    this.delimiter = builder.delimiter;
    this.includePrefix = builder.includePrefix;
    this.maxResult = builder.maxResults;
    this.fields = builder.fields;
  }

  /** Delimiter to use (typically {@code /}), otherwise {@code null}. */
  @Nullable
  String getDelimiter() {
    return delimiter;
  }

  /** Whether to include prefix object in the result. */
  boolean isIncludePrefix() {
    return includePrefix;
  }

  /** Maximum number of results to return, unlimited if negative or zero. */
  long getMaxResults() {
    return maxResult;
  }

  /**
   * Comma separated list of object fields to include in the list response.
   *
   * <p>See <a
   * href="https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations">
   * object resource</a> for reference.
   */
  @Nullable
  String getFields() {
    return fields;
  }

  static class Builder {
    private static final int MAX_RESULTS_UNLIMITED = -1;

    static final String OBJECT_FIELDS =
            String.join(
                    /* delimiter= */ ",",
                    "bucket",
                    "name",
                    "timeCreated",
                    "updated",
                    "generation",
                    "metageneration",
                    "size",
                    "contentType",
                    "contentEncoding",
                    "md5Hash",
                    "crc32c",
                    "metadata");

    private String delimiter;
    private boolean includePrefix;

    private long maxResults;

    private String fields;

    Builder() {
      this.delimiter = PATH_DELIMITER;
      this.includePrefix = false;
      this.maxResults = MAX_RESULTS_UNLIMITED;
      this.fields = OBJECT_FIELDS;
    }
    public Builder setDelimiter(String d) {
      this.delimiter = d;
      return this;
    }

    public Builder setIncludePrefix(boolean value) {
      this.includePrefix = value;
      return this;
    }

    public Builder setMaxResults(long mr) {
      this.maxResults = mr;
      return this;
    }

    public Builder setFields(String f) {
      this.fields = f;
      return this;
    }

    public ListObjectOptions build() {
      return new ListObjectOptions(this);
    }
  }
}
