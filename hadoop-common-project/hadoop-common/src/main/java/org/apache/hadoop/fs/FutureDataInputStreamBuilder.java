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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Builder for input streams and subclasses whose return value is
 * actually a completable future: this allows for better asynchronous
 * operation.
 *
 * To be more generic, {@link #opt(String, int)} and {@link #must(String, int)}
 * variants provide implementation-agnostic way to customize the builder.
 * Each FS-specific builder implementation can interpret the FS-specific
 * options accordingly, for example:
 *
 * If the option is not related to the file system, the option will be ignored.
 * If the option is must, but not supported by the file system, a
 * {@link IllegalArgumentException} will be thrown.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface FutureDataInputStreamBuilder
    extends FSBuilder<CompletableFuture<FSDataInputStream>, FutureDataInputStreamBuilder> {

  @Override
  CompletableFuture<FSDataInputStream> build()
      throws IllegalArgumentException, UnsupportedOperationException,
      IOException;

  /**
   * A FileStatus may be provided to the open request.
   * It is up to the implementation whether to use this or not.
   * @param status status.
   * @return the builder.
   */
  default FutureDataInputStreamBuilder withFileStatus(FileStatus status) {
    return this;
  }

}
