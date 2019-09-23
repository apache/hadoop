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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Read-specific operation context struct.
 */
public class S3AReadOpContext extends S3AOpContext {

  /**
   * Path of read.
   */
  private final Path path;

  /**
   * Initial input policy of the stream.
   */
  private final S3AInputPolicy inputPolicy;

  /**
   * How to detect and deal with the object being updated during read.
   */
  private final ChangeDetectionPolicy changeDetectionPolicy;

  /**
   * Readahead for GET operations/skip, etc.
   */
  private final long readahead;

  /**
   * Instantiate.
   * @param path path of read
   * @param isS3GuardEnabled true iff S3Guard is enabled.
   * @param invoker invoker for normal retries.
   * @param s3guardInvoker S3Guard-specific retry invoker.
   * @param stats statistics (may be null)
   * @param instrumentation FS instrumentation
   * @param dstFileStatus target file status
   * @param inputPolicy the input policy
   * @param readahead readahead for GET operations/skip, etc.
   * @param changeDetectionPolicy change detection policy.
   */
  public S3AReadOpContext(
      final Path path,
      boolean isS3GuardEnabled,
      Invoker invoker,
      Invoker s3guardInvoker,
      @Nullable FileSystem.Statistics stats,
      S3AInstrumentation instrumentation,
      FileStatus dstFileStatus,
      S3AInputPolicy inputPolicy,
      ChangeDetectionPolicy changeDetectionPolicy,
      final long readahead) {
    super(isS3GuardEnabled, invoker, s3guardInvoker, stats, instrumentation,
        dstFileStatus);
    this.path = checkNotNull(path);
    Preconditions.checkArgument(readahead >= 0,
        "invalid readahead %d", readahead);
    this.inputPolicy = checkNotNull(inputPolicy);
    this.changeDetectionPolicy = checkNotNull(changeDetectionPolicy);
    this.readahead = readahead;
  }

  /**
   * Get invoker to use for read operations.
   * When S3Guard is enabled we use the S3Guard invoker,
   * which deals with things like FileNotFoundException
   * differently.
   * @return invoker to use for read codepaths
   */
  public Invoker getReadInvoker() {
    if (isS3GuardEnabled) {
      return s3guardInvoker;
    } else {
      return invoker;
    }
  }

  /**
   * Get the path of this read.
   * @return path.
   */
  public Path getPath() {
    return path;
  }

  /**
   * Get the IO policy.
   * @return the initial input policy.
   */
  public S3AInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  public ChangeDetectionPolicy getChangeDetectionPolicy() {
    return changeDetectionPolicy;
  }

  /**
   * Get the readahead for this operation.
   * @return a value {@literal >=} 0
   */
  public long getReadahead() {
    return readahead;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AReadOpContext{");
    sb.append("path=").append(path);
    sb.append(", inputPolicy=").append(inputPolicy);
    sb.append(", readahead=").append(readahead);
    sb.append(", changeDetectionPolicy=").append(changeDetectionPolicy);
    sb.append('}');
    return sb.toString();
  }
}
