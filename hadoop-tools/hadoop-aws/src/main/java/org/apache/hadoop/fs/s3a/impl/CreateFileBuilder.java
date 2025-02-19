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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags;
import org.apache.hadoop.util.Progressable;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG;
import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONTENT_TYPE;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_HEADER;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.CONTENT_TYPE;
import static org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags.ConditionalOverwrite;
import static org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags.ConditionalOverwriteEtag;
import static org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags.CreateMultipart;
import static org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags.Performance;
import static org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags.Recursive;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.CREATE_FILE_KEYS;
import static org.apache.hadoop.util.Preconditions.checkArgument;

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
      new CreateFileOptions(CREATE_OVERWRITE_FLAGS,
          EnumSet.of(Recursive),
          null, null);

  /**
   * Classic create file option set: no overwrite.
   */
  public static final CreateFileOptions OPTIONS_CREATE_FILE_NO_OVERWRITE =
      new CreateFileOptions(CREATE_NO_OVERWRITE_FLAGS,
          EnumSet.of(Recursive),
          null, null);

  /**
   * Performance create options.
   */
  public static final CreateFileOptions OPTIONS_CREATE_FILE_PERFORMANCE =
      new CreateFileOptions(CREATE_OVERWRITE_FLAGS,
          EnumSet.of(Performance,Recursive),
          null, null);

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
    final EnumSet<WriteObjectFlags> createFileSwitches = EnumSet.noneOf(
        WriteObjectFlags.class);

    // pick up all headers from the mandatory list and strip them before
    // validating the keys

    // merge the config lists

    String headerPrefix = FS_S3A_CREATE_HEADER + ".";
    final int prefixLen = headerPrefix.length();

    final Set<String> keysToValidate = mandatoryKeys.stream()
        .filter(key -> !key.startsWith(headerPrefix))
        .collect(Collectors.toSet());

    rejectUnknownMandatoryKeys(keysToValidate, CREATE_FILE_KEYS, "for " + path);

    // look for headers

    for (Map.Entry<String, String> option : options) {
      String key = option.getKey();
      if (key.startsWith(headerPrefix) && key.length() > prefixLen) {
        headers.put(key.substring(prefixLen), option.getValue());
      }
    }

    // and add the mimetype
    if (options.get(FS_OPTION_CREATE_CONTENT_TYPE, null) != null)  {
      headers.put(CONTENT_TYPE, options.get(FS_OPTION_CREATE_CONTENT_TYPE, null));
    }

    EnumSet<CreateFlag> flags = getFlags();
    if (flags.contains(CreateFlag.APPEND)) {
      throw new UnsupportedOperationException("Append is not supported");
    }

    if (!flags.contains(CreateFlag.CREATE) &&
        !flags.contains(CreateFlag.OVERWRITE)) {
      throw new PathIOException(path.toString(),
          "Must specify either create or overwrite");
    }

    // build the other switches
    if (isRecursive()) {
      createFileSwitches.add(Recursive);
    }
    if (Performance.isEnabled(options)) {
      createFileSwitches.add(Performance);
    }
    if (CreateMultipart.isEnabled(options)) {
      createFileSwitches.add(CreateMultipart);
    }
    if (ConditionalOverwrite.isEnabled(options)) {
      createFileSwitches.add(ConditionalOverwrite);
    }
    // etag is a string so is checked for then extracted.
    final String etag = options.get(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG, null);
    if (etag != null) {
      createFileSwitches.add(ConditionalOverwriteEtag);
    }

    return callbacks.createFileFromBuilder(
        path,
        getProgress(),
        new CreateFileOptions(flags,
            createFileSwitches,
            etag,
            headers));
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
     * Create File switches.
     */
    private final EnumSet<WriteObjectFlags> writeObjectFlags;

    /**
     * Etag. Only used if the create file switches enable it.
     */
    private final String etag;

    /**
     * Headers; may be null.
     */
    private final Map<String, String> headers;

    /**
     * @param flags creation flags
     * @param writeObjectFlags Create File switches.
     * @param etag ETag, used only if enabled by switches
     * @param headers nullable header map.
     */
    public CreateFileOptions(
        final EnumSet<CreateFlag> flags,
        final EnumSet<WriteObjectFlags> writeObjectFlags,
        final String etag,
        final Map<String, String> headers) {
      this.flags = requireNonNull(flags);
      this.writeObjectFlags = requireNonNull(writeObjectFlags);
      if (writeObjectFlags().contains(ConditionalOverwriteEtag)) {
        checkArgument(!isEmpty(etag),
            "etag overwrite is enabled but the etag string is null/empty");
      }
      this.etag = etag;
      this.headers = headers;
    }

    @Override
    public String toString() {
      return "CreateFileOptions{" +
          "flags=" + flags +
          ", writeObjectFlags=" + writeObjectFlags +
          ", headers=" + headers +
          '}';
    }

    public EnumSet<CreateFlag> getFlags() {
      return flags;
    }

    public boolean isRecursive() {
      return isSet(Recursive);
    }

    public boolean isPerformance() {
      return isSet(Performance);
    }

    public boolean isConditionalOverwrite() {
      return isSet(ConditionalOverwrite);
    }

    public boolean isConditionalOverwriteEtag() {
      return isSet(ConditionalOverwriteEtag);
    }

    public boolean isSet(WriteObjectFlags val) {
      return writeObjectFlags().contains(val);
    }

    public Map<String, String> getHeaders() {
      return headers;
    }

    public String etag() {
      return etag;
    }

    public EnumSet<WriteObjectFlags> writeObjectFlags() {
      return writeObjectFlags;
    }
  }

}
