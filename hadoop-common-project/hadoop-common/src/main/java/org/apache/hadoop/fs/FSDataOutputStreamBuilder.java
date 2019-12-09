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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

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
    <S extends FSDataOutputStream, B extends FSDataOutputStreamBuilder<S, B>> {
  private final FileSystem fs;
  private final Path path;
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
   * Contains optional and mandatory parameters.
   *
   * It does not load default configurations from default files.
   */
  private final Configuration options = new Configuration(false);

  /** Keep track of the keys for mandatory options. */
  private final Set<String> mandatoryKeys = new HashSet<>();

  /**
   * Return the concrete implementation of the builder instance.
   */
  protected abstract B getThisBuilder();

  /**
   * Construct from a {@link FileContext}.
   *
   * @param fc FileContext
   * @param p path.
   * @throws IOException
   */
  FSDataOutputStreamBuilder(@Nonnull FileContext fc,
      @Nonnull Path p) throws IOException {
    Preconditions.checkNotNull(fc);
    Preconditions.checkNotNull(p);
    this.fs = null;
    this.path = p;

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
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(p);
    fs = fileSystem;
    path = p;
    bufferSize = fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
        IO_FILE_BUFFER_SIZE_DEFAULT);
    replication = fs.getDefaultReplication(path);
    blockSize = fs.getDefaultBlockSize(p);
  }

  protected FileSystem getFS() {
    Preconditions.checkNotNull(fs);
    return fs;
  }

  protected Path getPath() {
    return path;
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
    Preconditions.checkNotNull(perm);
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
    Preconditions.checkNotNull(prog);
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
    Preconditions.checkNotNull(chksumOpt);
    checksumOpt = chksumOpt;
    return getThisBuilder();
  }

  /**
   * Set optional Builder parameter.
   */
  public B opt(@Nonnull final String key, @Nonnull final String value) {
    mandatoryKeys.remove(key);
    options.set(key, value);
    return getThisBuilder();
  }

  /**
   * Set optional boolean parameter for the Builder.
   *
   * @see #opt(String, String)
   */
  public B opt(@Nonnull final String key, boolean value) {
    mandatoryKeys.remove(key);
    options.setBoolean(key, value);
    return getThisBuilder();
  }

  /**
   * Set optional int parameter for the Builder.
   *
   * @see #opt(String, String)
   */
  public B opt(@Nonnull final String key, int value) {
    mandatoryKeys.remove(key);
    options.setInt(key, value);
    return getThisBuilder();
  }

  /**
   * Set optional float parameter for the Builder.
   *
   * @see #opt(String, String)
   */
  public B opt(@Nonnull final String key, float value) {
    mandatoryKeys.remove(key);
    options.setFloat(key, value);
    return getThisBuilder();
  }

  /**
   * Set optional double parameter for the Builder.
   *
   * @see #opt(String, String)
   */
  public B opt(@Nonnull final String key, double value) {
    mandatoryKeys.remove(key);
    options.setDouble(key, value);
    return getThisBuilder();
  }

  /**
   * Set an array of string values as optional parameter for the Builder.
   *
   * @see #opt(String, String)
   */
  public B opt(@Nonnull final String key, @Nonnull final String... values) {
    mandatoryKeys.remove(key);
    options.setStrings(key, values);
    return getThisBuilder();
  }

  /**
   * Set mandatory option to the Builder.
   *
   * If the option is not supported or unavailable on the {@link FileSystem},
   * the client should expect {@link #build()} throws IllegalArgumentException.
   */
  public B must(@Nonnull final String key, @Nonnull final String value) {
    mandatoryKeys.add(key);
    options.set(key, value);
    return getThisBuilder();
  }

  /**
   * Set mandatory boolean option.
   *
   * @see #must(String, String)
   */
  public B must(@Nonnull final String key, boolean value) {
    mandatoryKeys.add(key);
    options.setBoolean(key, value);
    return getThisBuilder();
  }

  /**
   * Set mandatory int option.
   *
   * @see #must(String, String)
   */
  public B must(@Nonnull final String key, int value) {
    mandatoryKeys.add(key);
    options.setInt(key, value);
    return getThisBuilder();
  }

  /**
   * Set mandatory float option.
   *
   * @see #must(String, String)
   */
  public B must(@Nonnull final String key, float value) {
    mandatoryKeys.add(key);
    options.setFloat(key, value);
    return getThisBuilder();
  }

  /**
   * Set mandatory double option.
   *
   * @see #must(String, String)
   */
  public B must(@Nonnull final String key, double value) {
    mandatoryKeys.add(key);
    options.setDouble(key, value);
    return getThisBuilder();
  }

  /**
   * Set a string array as mandatory option.
   *
   * @see #must(String, String)
   */
  public B must(@Nonnull final String key, @Nonnull final String... values) {
    mandatoryKeys.add(key);
    options.setStrings(key, values);
    return getThisBuilder();
  }

  protected Configuration getOptions() {
    return options;
  }

  /**
   * Get all the keys that are set as mandatory keys.
   */
  @VisibleForTesting
  protected Set<String> getMandatoryKeys() {
    return Collections.unmodifiableSet(mandatoryKeys);
  }

  /**
   * Create the FSDataOutputStream to write on the file system.
   *
   * @throws IllegalArgumentException if the parameters are not valid.
   * @throws IOException on errors when file system creates or appends the file.
   */
  public abstract S build() throws IllegalArgumentException, IOException;
}
