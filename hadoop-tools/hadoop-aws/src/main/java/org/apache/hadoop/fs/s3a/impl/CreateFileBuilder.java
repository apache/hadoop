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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.util.EnumSet;
import javax.annotation.Nonnull;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.s3a.impl.InternalConstants.CREATE_FILE_KEYS;

/**
 * Builder used in create file; takes a callback to the operation
 * to create the file.
 * Is non-recursive unless explicitly changed.
 */
public class CreateFileBuilder extends
    FSDataOutputStreamBuilder<FSDataOutputStream, CreateFileBuilder> {

  public static final EnumSet<CreateFlag> CREATE_OVERWRITE =
      EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);

  public static final EnumSet<CreateFlag> CREATE_NO_OVERWRITE =
      EnumSet.of(CreateFlag.CREATE);

  /**
   * Callback interface.
   */
  private final CreateFileBuilderCallbacks callbacks;

  /**
   * Constructor.
   * @param fileSystem fs; used by superclass.
   * @param path qualified path to create
   * @param callbacks callbacks.
   */
  public CreateFileBuilder(
      @Nonnull final FileSystem fileSystem,
      @Nonnull final Path path,
      @Nonnull final CreateFileBuilderCallbacks callbacks) {

    super(fileSystem, path);
    this.callbacks = callbacks;
  }

  @Override
  public CreateFileBuilder getThisBuilder() {
    return this;
  }

  @Override
  public FSDataOutputStream build() throws IOException {
    Path path = getPath();
    rejectUnknownMandatoryKeys(CREATE_FILE_KEYS,
        " for " + path);
    EnumSet<CreateFlag> flags = getFlags();
    if (flags.contains(CreateFlag.APPEND)) {
      throw new UnsupportedOperationException("Append is not supported");
    }
    if (!flags.contains(CreateFlag.CREATE) &&
        !flags.contains(CreateFlag.OVERWRITE)) {
      throw new PathIOException(path.toString(),
          "Must specify either create or overwrite");
    }

    return callbacks.createFileFromBuilder(
        path,
        flags,
        isRecursive(),
        getProgress(),
        getOptions().getBoolean(Constants.FS_S3A_CREATE_PERFORMANCE, false));

  }

  /**
   * Pass flags down.
   * @param flags input flags.
   * @return this builder.
   */
  public CreateFileBuilder withFlags(EnumSet<CreateFlag> flags) {
    if (flags.contains(CreateFlag.CREATE)) {
      create();
    }
    if (flags.contains(CreateFlag.APPEND)) {
      append();
    }
    overwrite(flags.contains(CreateFlag.OVERWRITE));
    return this;
  }

  /**
   * make the flag getter public.
   * @return creation flags.
   */
  public EnumSet<CreateFlag> getFlags() {
    return super.getFlags();
  }

  /**
   * Callbacks for creating the file.
   */
  public interface CreateFileBuilderCallbacks {

    /**
     * Create a file from the builder.
     *
     * @param path path to file
     * @param flags creation flags
     * @param recursive create parent dirs?
     * @param progress progress callback
     * @param performance performance flag
     * @return the stream
     * @throws IOException any IO problem
     */
    FSDataOutputStream createFileFromBuilder(
        Path path,
        EnumSet<CreateFlag> flags,
        boolean recursive,
        Progressable progress,
        boolean performance) throws IOException;
  }
}
