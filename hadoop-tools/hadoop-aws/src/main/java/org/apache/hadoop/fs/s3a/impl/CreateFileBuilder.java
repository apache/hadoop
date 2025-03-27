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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_HEADER;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.CREATE_FILE_KEYS;

/**
 * Builder used in create file; takes a callback to the operation
 * to create the file.
 * Is non-recursive unless explicitly changed.
 */
public class CreateFileBuilder extends
    FSDataOutputStreamBuilder<FSDataOutputStream, CreateFileBuilder> {

  /**
   * Flag set to create with overwrite.
   */
  public static final EnumSet<CreateFlag> CREATE_OVERWRITE_FLAGS =
      EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);

  /**
    * Flag set to create without overwrite.
   */
  public static final EnumSet<CreateFlag> CREATE_NO_OVERWRITE_FLAGS =
      EnumSet.of(CreateFlag.CREATE);

  /**
   * Classic create file option set: overwriting.
   */
  public static final CreateFileOptions OPTIONS_CREATE_FILE_OVERWRITE =
      new CreateFileOptions(CREATE_OVERWRITE_FLAGS, true, false, null);

  /**
   * Classic create file option set: no overwrite.
   */
  public static final CreateFileOptions OPTIONS_CREATE_FILE_NO_OVERWRITE =
      new CreateFileOptions(CREATE_NO_OVERWRITE_FLAGS, true, false, null);

  /**
   * Performance create options.
   */
  public static final CreateFileOptions OPTIONS_CREATE_FILE_PERFORMANCE =
      new CreateFileOptions(CREATE_OVERWRITE_FLAGS, true, true, null);

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

    final Configuration options = getOptions();
    final Map<String, String> headers = new HashMap<>();
    final Set<String> mandatoryKeys = getMandatoryKeys();
    final Set<String> keysToValidate = new HashSet<>();

    // pick up all headers from the mandatory list and strip them before
    // validating the keys
    String headerPrefix = FS_S3A_CREATE_HEADER + ".";
    final int prefixLen = headerPrefix.length();
    mandatoryKeys.stream().forEach(key -> {
      if (key.startsWith(headerPrefix) && key.length() > prefixLen) {
        headers.put(key.substring(prefixLen), options.get(key));
      } else {
        keysToValidate.add(key);
      }
    });

    rejectUnknownMandatoryKeys(keysToValidate, CREATE_FILE_KEYS, "for " + path);

    // and add any optional headers
    getOptionalKeys().stream()
        .filter(key -> key.startsWith(headerPrefix) && key.length() > prefixLen)
        .forEach(key -> headers.put(key.substring(prefixLen), options.get(key)));


    EnumSet<CreateFlag> flags = getFlags();
    if (flags.contains(CreateFlag.APPEND)) {
      throw new UnsupportedOperationException("Append is not supported");
    }

    if (!flags.contains(CreateFlag.CREATE) &&
        !flags.contains(CreateFlag.OVERWRITE)) {
      throw new PathIOException(path.toString(),
          "Must specify either create or overwrite");
    }

    final boolean performance =
        options.getBoolean(Constants.FS_S3A_CREATE_PERFORMANCE, false);
    return callbacks.createFileFromBuilder(
        path,
        getProgress(),
        new CreateFileOptions(flags, isRecursive(), performance, headers));

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
     * @param path path to file
     * @param progress progress callback
     * @param options options for the file
     * @return the stream
     * @throws IOException any IO problem
     */
    FSDataOutputStream createFileFromBuilder(
        Path path,
        Progressable progress,
        CreateFileOptions options) throws IOException;
  }

  /**
   * Create file options as built from the builder set or the classic
   * entry point.
   */
  public static final class CreateFileOptions {

    /**
     * creation flags.
     * create parent dirs?
     * progress callback.
     * performance flag.
     */
    private final EnumSet<CreateFlag> flags;

    /**
     * create parent dirs?
     */
    private final boolean recursive;

    /**
     * performance flag.
     */
    private final boolean performance;

    /**
     * Headers; may be null.
     */
    private final Map<String, String> headers;

    /**
     * @param flags creation flags
     * @param recursive create parent dirs?
     * @param performance performance flag
     * @param headers nullable header map.
     */
    public CreateFileOptions(
        final EnumSet<CreateFlag> flags,
        final boolean recursive,
        final boolean performance,
        final Map<String, String> headers) {
      this.flags = flags;
      this.recursive = recursive;
      this.performance = performance;
      this.headers = headers;
    }

    @Override
    public String toString() {
      return "CreateFileOptions{" +
          "flags=" + flags +
          ", recursive=" + recursive +
          ", performance=" + performance +
          ", headers=" + headers +
          '}';
    }

    public EnumSet<CreateFlag> getFlags() {
      return flags;
    }

    public boolean isRecursive() {
      return recursive;
    }

    public boolean isPerformance() {
      return performance;
    }

    public Map<String, String> getHeaders() {
      return headers;
    }
  }

}
