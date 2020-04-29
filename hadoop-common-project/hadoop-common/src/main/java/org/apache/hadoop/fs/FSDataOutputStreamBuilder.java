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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.EnumSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * Builder for {@link FSDataOutputStream} and its subclasses.
 *
 * It is used to create {@link FSDataOutputStream} when creating a new file or
 * appending an existing file on {@link FileSystem}.
 *
 * By default, it does not create parent directory that do not exist.
 * {@link FileSystem#createNonRecursive(Path, boolean, int, short, long,
 * Progressable)}.
 *
 * To create missing parent directory, use {@link #recursive()}.
 *
 * To be more generic, {@link #opt(String, int)} and {@link #must(String, int)}
 * variants provide implementation-agnostic way to customize the builder.
 * Each FS-specific builder implementation can interpret the FS-specific
 * options accordingly, for example:
 *
 * <code>
 *
 * // Don't
 * if (fs instanceof FooFileSystem) {
 *   FooFileSystem fs = (FooFileSystem) fs;
 *   OutputStream out = dfs.createFile(path)
 *     .optionA()
 *     .optionB("value")
 *     .cache()
 *   .build()
 * } else if (fs instanceof BarFileSystem) {
 *   ...
 * }
 *
 * // Do
 * OutputStream out = fs.createFile(path)
 *   .permission(perm)
 *   .bufferSize(bufSize)
 *   .opt("foofs:option.a", true)
 *   .opt("foofs:option.b", "value")
 *   .opt("barfs:cache", true)
 *   .must("foofs:cache", true)
 *   .must("barfs:cache-size", 256 * 1024 * 1024)
 *   .build();
 * </code>
 *
 * If the option is not related to the file system, the option will be ignored.
 * If the option is must, but not supported by the file system, a
 * {@link IllegalArgumentException} will be thrown.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class FSDataOutputStreamBuilder
    <S extends FSDataOutputStream, B extends FSDataOutputStreamBuilder<S, B>>
    extends AbstractFSBuilderImpl<S, B> {
  private final FileSystem fs;
  private FsPermission permission = null;
  private int bufferSize;
  private short replication;
  private long blockSize;
  /** set to true to create missing directory. */
  private boolean recursive = false;
  private final EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
  private Progressable progress = null;
  private ChecksumOpt checksumOpt = null;

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
  FSDataOutputStreamBuilder(@Nonnull FileContext fc,
      @Nonnull Path p) throws IOException {
    super(checkNotNull(p));
    checkNotNull(fc);
    this.fs = null;

    AbstractFileSystem afs = fc.getFSofPath(p);
    FsServerDefaults defaults = afs.getServerDefaults(p);
    bufferSize = defaults.getFileBufferSize();
    replication = defaults.getReplication();
    blockSize = defaults.getBlockSize();
  }

  /**
   * Constructor.
   */
  protected FSDataOutputStreamBuilder(@Nonnull FileSystem fileSystem,
      @Nonnull Path p) {
    super(checkNotNull(p));
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
  public B blockSize(long blkSize) {
    blockSize = blkSize;
    return getThisBuilder();
  }

  /**
   * Return true to create the parent directories if they do not exist.
   */
  protected boolean isRecursive() {
    return recursive;
  }

  /**
   * Create the parent directory if they do not exist.
   */
  public B recursive() {
    recursive = true;
    return getThisBuilder();
  }

  protected Progressable getProgress() {
    return progress;
  }

  /**
   * Set the facility of reporting progress.
   */
  public B progress(@Nonnull final Progressable prog) {
    checkNotNull(prog);
    progress = prog;
    return getThisBuilder();
  }

  protected EnumSet<CreateFlag> getFlags() {
    return flags;
  }

  /**
   * Create an FSDataOutputStream at the specified path.
   */
  public B create() {
    flags.add(CreateFlag.CREATE);
    return getThisBuilder();
  }

  /**
   * Set to true to overwrite the existing file.
   * Set it to false, an exception will be thrown when calling {@link #build()}
   * if the file exists.
   */
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
  public B checksumOpt(@Nonnull final ChecksumOpt chksumOpt) {
    checkNotNull(chksumOpt);
    checksumOpt = chksumOpt;
    return getThisBuilder();
  }

  /**
   * Create the FSDataOutputStream to write on the file system.
   *
   * @throws IllegalArgumentException if the parameters are not valid.
   * @throws IOException on errors when file system creates or appends the file.
   */
  public abstract S build() throws IllegalArgumentException, IOException;
}
