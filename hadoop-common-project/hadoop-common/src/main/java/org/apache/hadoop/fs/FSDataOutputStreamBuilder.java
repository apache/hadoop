/**
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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.EnumSet;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/** Base of specific file system FSDataOutputStreamBuilder. */
public class FSDataOutputStreamBuilder{
  private Path path = null;
  private FsPermission permission = null;
  private Integer bufferSize;
  private Short replication;
  private Long blockSize;
  private Progressable progress = null;
  private EnumSet<CreateFlag> flags = null;
  private ChecksumOpt checksumOpt = null;

  private final FileSystem fs;

  public FSDataOutputStreamBuilder(FileSystem fileSystem, Path p) {
    fs = fileSystem;
    path = p;
  }

  protected Path getPath() {
    return path;
  }

  protected FsPermission getPermission() {
    if (permission == null) {
      return FsPermission.getFileDefault();
    }
    return permission;
  }

  public FSDataOutputStreamBuilder setPermission(final FsPermission perm) {
    Preconditions.checkNotNull(perm);
    permission = perm;
    return this;
  }

  protected int getBufferSize() {
    if (bufferSize == null) {
      return fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
          IO_FILE_BUFFER_SIZE_DEFAULT);
    }
    return bufferSize;
  }

  public FSDataOutputStreamBuilder setBufferSize(int bufSize) {
    bufferSize = bufSize;
    return this;
  }

  protected short getReplication() {
    if (replication == null) {
      return fs.getDefaultReplication(getPath());
    }
    return replication;
  }

  public FSDataOutputStreamBuilder setReplication(short replica) {
    replication = replica;
    return this;
  }

  protected long getBlockSize() {
    if (blockSize == null) {
      return fs.getDefaultBlockSize(getPath());
    }
    return blockSize;
  }

  public FSDataOutputStreamBuilder setBlockSize(long blkSize) {
    blockSize = blkSize;
    return this;
  }

  protected Progressable getProgress() {
    return progress;
  }

  public FSDataOutputStreamBuilder setProgress(final Progressable prog) {
    Preconditions.checkNotNull(prog);
    progress = prog;
    return this;
  }

  protected EnumSet<CreateFlag> getFlags() {
    if (flags == null) {
      return EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    }
    return flags;
  }

  public FSDataOutputStreamBuilder setFlags(
      final EnumSet<CreateFlag> enumFlags) {
    Preconditions.checkNotNull(enumFlags);
    flags = enumFlags;
    return this;
  }

  protected ChecksumOpt getChecksumOpt() {
    return checksumOpt;
  }

  public FSDataOutputStreamBuilder setChecksumOpt(
      final ChecksumOpt chksumOpt) {
    Preconditions.checkNotNull(chksumOpt);
    checksumOpt = chksumOpt;
    return this;
  }

  public FSDataOutputStream build() throws IOException {
    return fs.create(getPath(), getPermission(), getFlags(), getBufferSize(),
        getReplication(), getBlockSize(), getProgress(), getChecksumOpt());
  }
}
