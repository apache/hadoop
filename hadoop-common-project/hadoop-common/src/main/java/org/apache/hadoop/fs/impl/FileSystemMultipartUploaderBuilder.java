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
import java.util.EnumSet;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Builder for {@link FileSystemMultipartUploader}.
 */
public class FileSystemMultipartUploaderBuilder extends
    MultipartUploaderBuilderImpl<FileSystemMultipartUploader, FileSystemMultipartUploaderBuilder> {

  public FileSystemMultipartUploaderBuilder(
      @Nonnull final FileSystem fileSystem,
      @Nonnull final Path path) {
    super(fileSystem, path);
  }

  @Override
  public FileSystemMultipartUploaderBuilder getThisBuilder() {
    return this;
  }

  @Override
  public FileSystemMultipartUploader build()
      throws IllegalArgumentException, IOException {
    return new FileSystemMultipartUploader(this, getFS());
  }

  @Override
  public FileSystem getFS() {
    return super.getFS();
  }

  @Override
  public FsPermission getPermission() {
    return super.getPermission();
  }

  @Override
  public int getBufferSize() {
    return super.getBufferSize();
  }

  @Override
  public short getReplication() {
    return super.getReplication();
  }

  @Override
  public EnumSet<CreateFlag> getFlags() {
    return super.getFlags();
  }

  @Override
  public Options.ChecksumOpt getChecksumOpt() {
    return super.getChecksumOpt();
  }

  @Override
  protected long getBlockSize() {
    return super.getBlockSize();
  }


}
