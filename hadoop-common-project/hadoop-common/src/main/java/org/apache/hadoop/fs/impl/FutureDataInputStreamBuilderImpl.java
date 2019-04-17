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

package org.apache.hadoop.fs.impl;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

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
public abstract class FutureDataInputStreamBuilderImpl
    extends AbstractFSBuilderImpl<CompletableFuture<FSDataInputStream>, FutureDataInputStreamBuilder>
    implements FutureDataInputStreamBuilder {

  private final FileSystem fileSystem;

  private int bufferSize;

  /**
   * Construct from a {@link FileContext}.
   *
   * @param fc FileContext
   * @param path path.
   * @throws IOException failure
   */
  protected FutureDataInputStreamBuilderImpl(@Nonnull FileContext fc,
      @Nonnull Path path) throws IOException {
    super(checkNotNull(path));
    checkNotNull(fc);
    this.fileSystem = null;
    bufferSize = IO_FILE_BUFFER_SIZE_DEFAULT;
  }

  /**
   * Constructor.
   * @param fileSystem owner FS.
   * @param path path
   */
  protected FutureDataInputStreamBuilderImpl(@Nonnull FileSystem fileSystem,
      @Nonnull Path path) {
    super(checkNotNull(path));
    this.fileSystem = checkNotNull(fileSystem);
    initFromFS();
  }

  /**
   * Constructor with PathHandle.
   * @param fileSystem owner FS.
   * @param pathHandle path handle
   */
  public FutureDataInputStreamBuilderImpl(@Nonnull FileSystem fileSystem,
      @Nonnull PathHandle pathHandle) {
    super(pathHandle);
    this.fileSystem = fileSystem;
    initFromFS();
  }

  /**
   * Initialize from a filesystem.
   */
  private void initFromFS() {
    bufferSize = fileSystem.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
        IO_FILE_BUFFER_SIZE_DEFAULT);
  }

  protected FileSystem getFS() {
    checkNotNull(fileSystem);
    return fileSystem;
  }

  protected int getBufferSize() {
    return bufferSize;
  }

  /**
   * Set the size of the buffer to be used.
   */
  public FutureDataInputStreamBuilder bufferSize(int bufSize) {
    bufferSize = bufSize;
    return getThisBuilder();
  }

  /**
   * Get the builder.
   * This must be used after the constructor has been invoked to create
   * the actual builder: it allows for subclasses to do things after
   * construction.
   */
  public FutureDataInputStreamBuilder builder() {
    return getThisBuilder();
  }

  @Override
  public FutureDataInputStreamBuilder getThisBuilder() {
    return this;
  }
}
