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

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Options that can be specified when creating a file in the {@link GoogleCloudStorageFileSystem}.
 */
class CreateOptions {
  private final ImmutableMap<String, byte[]> attributes;
  private final String contentType;
  private final boolean ensureNoDirectoryConflict;
  private final Duration interval;
  private final long overwriteGenerationId;
  private final WriteMode mode;

  public static final CreateOptions DEFAULT = builder().build();

  public String getContentEncoding() {
    return contentEncoding;
  }

  private final String contentEncoding;

  private CreateOptions(CreateOperationOptionsBuilder builder) {
    this.attributes = ImmutableMap.copyOf(builder.attributes);
    this.contentType = builder.contentType;
    this.ensureNoDirectoryConflict = builder.ensureNoDirectoryConflict;
    this.interval = builder.interval;
    this.overwriteGenerationId = builder.overwriteGenerationId;
    this.mode = builder.mode;
    this.contentEncoding = builder.contentEncoding;
  }

  public boolean isOverwriteExisting() {
    return this.mode == WriteMode.OVERWRITE;
  }

  enum WriteMode {
    /**
     * Write new bytes to the end of the existing file rather than the beginning.
     */
    APPEND,
    /**
     * Creates a new file for write and fails if file already exists.
     */
    CREATE_NEW,
    /**
     * Creates a new file for write or overwrites an existing file if it already exists.
     */
    OVERWRITE
  }

  public static CreateOperationOptionsBuilder builder() {
    return new CreateOperationOptionsBuilder();
  }

  /**
   * Extended attributes to set when creating a file.
   */
  public ImmutableMap<String, byte[]> getAttributes() {
    return attributes;
  }

  /**
   * Content-type to set when creating a file.
   */
  @Nullable
  public String getContentType() {
    return contentType;
  }

  /**
   * Configures the minimum time interval (milliseconds) between consecutive sync/flush calls
   */
  public Duration getMinSyncInterval() {
    return interval;
  }

  /**
   * If true, makes sure there isn't already a directory object of the same name. If false, you run
   * the risk of creating hard-to-cleanup/access files whose names collide with directory names. If
   * already sure no such directory exists, then this is safe to set for improved performance.
   */
  public boolean isEnsureNoDirectoryConflict() {
    return ensureNoDirectoryConflict;
  }

  /**
   * Whether to overwrite an existing file with the same name.
   */
  public WriteMode getWriteMode() {
    return mode;
  }

  /**
   * Generation of existing object to overwrite. Ignored if set to {@link
   * StorageResourceId#UNKNOWN_GENERATION_ID}, but otherwise this is used instead of {@code
   * overwriteExisting}, where 0 indicates no existing object, and otherwise an existing object will
   * only be overwritten by the newly created file if its generation matches this provided
   * generationId.
   */
  public long getOverwriteGenerationId() {
    return overwriteGenerationId;
  }

  static class CreateOperationOptionsBuilder {
    private Map<String, byte[]> attributes = ImmutableMap.of();
    private String contentType = "application/octet-stream";
    private boolean ensureNoDirectoryConflict = true;
    private Duration interval = Duration.ZERO;
    private long overwriteGenerationId = StorageResourceId.UNKNOWN_GENERATION_ID;
    private WriteMode mode = WriteMode.CREATE_NEW;

    private String contentEncoding = null;

    public CreateOperationOptionsBuilder setAttributes(Map<String, byte[]> attributes) {
      this.attributes = attributes;
      return this;
    }

    public CreateOperationOptionsBuilder setContentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public CreateOperationOptionsBuilder setEnsureNoDirectoryConflict(
        boolean ensureNoDirectoryConflict) {
      this.ensureNoDirectoryConflict = ensureNoDirectoryConflict;
      return this;
    }

    public CreateOperationOptionsBuilder setMinSyncInterval(Duration interval) {
      this.interval = interval;
      return this;
    }

    public CreateOperationOptionsBuilder setOverwriteGenerationId(long overwriteGenerationId) {
      this.overwriteGenerationId = overwriteGenerationId;
      return this;
    }

    public CreateOperationOptionsBuilder setWriteMode(WriteMode mode) {
      this.mode = mode;
      return this;
    }

    CreateOptions build() {
      CreateOptions options = new CreateOptions(this);

      checkArgument(!options.getAttributes().containsKey("Content-Type"),
          "The Content-Type attribute must be set via the contentType option");
      if (options.getWriteMode() != WriteMode.OVERWRITE) {
        checkArgument(options.getOverwriteGenerationId() == StorageResourceId.UNKNOWN_GENERATION_ID,
            "overwriteGenerationId is set to %s but it can be set only in OVERWRITE mode",
            options.getOverwriteGenerationId());
      }

      return options;
    }
  }
}
