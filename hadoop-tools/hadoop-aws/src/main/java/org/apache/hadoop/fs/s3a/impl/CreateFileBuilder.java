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
import java.util.Collections;
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
 * Builder used in create file; takes a callback to the method
 * which is actually invoked.
 */
public class CreateFileBuilder extends
    FSDataOutputStreamBuilder<FSDataOutputStream,
        CreateFileBuilder> {

  private final CreateFileBuilderCallbacks callbacks;

  public CreateFileBuilder(
      @Nonnull final FileSystem fileSystem,
      @Nonnull final Path p,
      final CreateFileBuilderCallbacks callbacks) {

    super(fileSystem, p);
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
    if (flags.contains(CreateFlag.CREATE) ||
        flags.contains(CreateFlag.OVERWRITE)) {

      return callbacks.createFileFromBuilder(
          path,
          flags.contains(CreateFlag.OVERWRITE),
          getProgress(),
          getOptions().getBoolean(Constants.FS_S3A_CREATE_PERFORMANCE, false));

    } else if (flags.contains(CreateFlag.APPEND)) {
      throw new UnsupportedOperationException("Append is not supported");
    }
    throw new PathIOException(path.toString(),
        "Must specify either create, overwrite or append");
  }

  /**
   * Callback into the FS for creating the file.
   */
  public interface CreateFileBuilderCallbacks {
    FSDataOutputStream createFileFromBuilder(
        Path path,
        boolean overwrite,
        Progressable progress,
        boolean performance) throws IOException;
  }
}
