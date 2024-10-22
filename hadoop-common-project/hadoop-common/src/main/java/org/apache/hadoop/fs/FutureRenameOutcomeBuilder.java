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
package org.apache.hadoop.fs;

import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Progressable;

/**
 * Builder for input streams and subclasses whose return value is
 * actually a completable future: this allows for better asynchronous
 * operation.
 * <p>
 * To be more generic, {@link #opt(String, int)} and {@link #must(String, int)}
 * variants provide implementation-agnostic way to customize the builder.
 * Each FS-specific builder implementation can interpret the FS-specific
 * options accordingly, for example:
 * <p>
 * If the option is not related to the file system, the option will be ignored.
 * If the option is must, but not supported/known by the file system, an
 * {@link IllegalArgumentException} will be thrown.
 * <p>
 * See {@see FileContext#rename(Path, Path, Options.Rename..)}.
 * See {@see FileSystem#rename(Path, Path)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface FutureRenameOutcomeBuilder
    extends FSBuilder<CompletableFuture<RenameOutcome>, FutureRenameOutcomeBuilder> {

  @Override
  default CompletableFuture<RenameOutcome> build()
      throws IllegalArgumentException, UnsupportedOperationException,
             UncheckedIOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  /**
   * An optional status of the source file.
   * The name of the status MUST match that of the source path;
   * the rest of the path SHALL NOT be compared.
   * It is up to the implementation whether to use this or not.
   * @param status status: may be null
   * @return the builder.
   */
  default FutureRenameOutcomeBuilder withSourceStatus(
      @Nullable FileStatus status) {
    return this;
  }

  default FutureRenameOutcomeBuilder requireAtomic(
      boolean flag) {
    return this;
  }

  default FutureRenameOutcomeBuilder withLegacyPathFixup(
      boolean flag) {
    return this;
  }

  default FutureRenameOutcomeBuilder withProgress(
      @Nullable Progressable progressable) {

    return this;
  }

  default FutureRenameOutcomeBuilder withSourceEtag(
      @Nullable String etag) {

    return this;
  }

  default FutureRenameOutcomeBuilder withSourceType(
      @Nullable Options.RenameSourceType type) {
    return this;
  }



}
