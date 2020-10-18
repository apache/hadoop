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

package org.apache.hadoop.fs.impl;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.MultipartUploaderBuilder;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * Builder for {@link MultipartUploader} implementations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class MultipartUploaderBuilderImpl
    <S extends MultipartUploader, B extends MultipartUploaderBuilder<S, B>>
    extends AbstractFSBuilderImpl<S, B>
    implements MultipartUploaderBuilder<S, B> {

  private final FileSystem fs;

  private FsPermission permission;

  private int bufferSize;

  private short replication;

  private long blockSize;

  private final EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);

  private ChecksumOpt checksumOpt;

  /**
   * Return the concrete implementation of the builder instance.
   */
  public abstract B getThisBuilder();

  /**
   * Construct from a {@link FileContext}.
   *
   * @param fc FileContext
   * @param p path.
   * @throws IOException failure
   */
  protected MultipartUploaderBuilderImpl(@Nonnull FileContext fc,
      @Nonnull Path p) throws IOException {
    super(checkNotNull(p));
    checkNotNull(fc);
    this.fs = null;

    FsServerDefaults defaults = fc.getServerDefaults(p);
    bufferSize = defaults.getFileBufferSize();
    replication = defaults.getReplication();
    blockSize = defaults.getBlockSize();
  }

  /**
   * Constructor.
   */
  protected MultipartUploaderBuilderImpl(@Nonnull FileSystem fileSystem,
      @Nonnull Path p) {
    super(fileSystem.makeQualified(checkNotNull(p)));
    checkNotNull(fileSystem);
    fs = fileSystem;
    bufferSize = fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
        IO_FILE_BUFFER_SIZE_DEFAULT);
    replication = fs.getDefaultReplication(p);
    blockSize = fs.getDefaultBlockSize(p);
  }

  protected FileSystem getFS() {
    checkNotNull(fs);
    return fs;
  }

  protected FsPermission getPermission() {
    if (permission == null) {
      permission = FsPermission.getFileDefault();
    }
    return permission;
  }

  /**
   * Set permission for the file.
   */
  @Override
  public B permission(@Nonnull final FsPermission perm) {
    checkNotNull(perm);
    permission = perm;
    return getThisBuilder();
  }

  protected int getBufferSize() {
    return bufferSize;
  }

  /**
   * Set the size of the buffer to be used.
   */
  @Override
  public B bufferSize(int bufSize) {
    bufferSize = bufSize;
    return getThisBuilder();
  }

  protected short getReplication() {
    return replication;
  }

  /**
   * Set replication factor.
   */
  @Override
  public B replication(short replica) {
    replication = replica;
    return getThisBuilder();
  }

  protected long getBlockSize() {
    return blockSize;
  }

  /**
   * Set block size.
   */
  @Override
  public B blockSize(long blkSize) {
    blockSize = blkSize;
    return getThisBuilder();
  }

  protected EnumSet<CreateFlag> getFlags() {
    return flags;
  }

  /**
   * Create an FSDataOutputStream at the specified path.
   */
  @Override
  public B create() {
    flags.add(CreateFlag.CREATE);
    return getThisBuilder();
  }

  /**
   * Set to true to overwrite the existing file.
   * Set it to false, an exception will be thrown when calling {@link #build()}
   * if the file exists.
   */
  @Override
  public B overwrite(boolean overwrite) {
    if (overwrite) {
      flags.add(CreateFlag.OVERWRITE);
    } else {
      flags.remove(CreateFlag.OVERWRITE);
    }
    return getThisBuilder();
  }

  /**
   * Append to an existing file (optional operation).
   */
  @Override
  public B append() {
    flags.add(CreateFlag.APPEND);
    return getThisBuilder();
  }

  protected ChecksumOpt getChecksumOpt() {
    return checksumOpt;
  }

  /**
   * Set checksum opt.
   */
  @Override
  public B checksumOpt(@Nonnull final ChecksumOpt chksumOpt) {
    checkNotNull(chksumOpt);
    checksumOpt = chksumOpt;
    return getThisBuilder();
  }

}
