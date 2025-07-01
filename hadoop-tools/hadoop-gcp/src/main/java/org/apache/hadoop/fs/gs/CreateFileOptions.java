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

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Options that can be specified when creating a file in the {@link GoogleCloudStorageFileSystem}.
 */
final class CreateFileOptions {
  static final CreateFileOptions DEFAULT = CreateFileOptions.builder().build();
  private final ImmutableMap<String, byte[]> attributes;
  private final String contentType;
  private final long overwriteGenerationId;
  private final WriteMode mode;
  private final boolean ensureNoDirectoryConflict;

  private CreateFileOptions(CreateOperationOptionsBuilder builder) {
    this.attributes = ImmutableMap.copyOf(builder.attributes);
    this.contentType = builder.contentType;
    this.overwriteGenerationId = builder.overwriteGenerationId;
    this.mode = builder.writeMode;
    this.ensureNoDirectoryConflict = builder.ensureNoDirectoryConflict;
  }

  boolean isOverwriteExisting() {
    return this.mode == WriteMode.OVERWRITE;
  }

  boolean isEnsureNoDirectoryConflict() {
    return ensureNoDirectoryConflict;
  }

  CreateOperationOptionsBuilder toBuilder() {
    return builder().setWriteMode(this.mode)
        .setEnsureNoDirectoryConflict(ensureNoDirectoryConflict);
  }

  enum WriteMode {
    /** Write new bytes to the end of the existing file rather than the beginning. */
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

  static CreateOperationOptionsBuilder builder() {
    return new CreateOperationOptionsBuilder();
  }

  /**
   * Extended attributes to set when creating a file.
   */
  ImmutableMap<String, byte[]> getAttributes() {
    return attributes;
  }

  /**
   * Content-type to set when creating a file.
   */
  @Nullable
  String getContentType() {
    return contentType;
  }

  /**
   * Whether to overwrite an existing file with the same name.
   */
  WriteMode getWriteMode() {
    return mode;
  }

  /**
   * Generation of existing object to overwrite. Ignored if set to {@link
   * StorageResourceId#UNKNOWN_GENERATION_ID}, but otherwise this is used instead of {@code
   * overwriteExisting}, where 0 indicates no existing object, and otherwise an existing object will
   * only be overwritten by the newly created file if its generation matches this provided
   * generationId.
   */
  long getOverwriteGenerationId() {
    return overwriteGenerationId;
  }

  static class CreateOperationOptionsBuilder {
    private Map<String, byte[]> attributes = ImmutableMap.of();
    private String contentType = "application/octet-stream";
    private long overwriteGenerationId = StorageResourceId.UNKNOWN_GENERATION_ID;
    private WriteMode writeMode = WriteMode.CREATE_NEW;
    private boolean ensureNoDirectoryConflict = true;

    CreateOperationOptionsBuilder setWriteMode(WriteMode mode) {
      this.writeMode = mode;
      return this;
    }

    CreateOperationOptionsBuilder setEnsureNoDirectoryConflict(boolean ensure) {
      this.ensureNoDirectoryConflict = ensure;
      return this;
    }

    CreateFileOptions build() {
      CreateFileOptions options = new CreateFileOptions(this);

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
