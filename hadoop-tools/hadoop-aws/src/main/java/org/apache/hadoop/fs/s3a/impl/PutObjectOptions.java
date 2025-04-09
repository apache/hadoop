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

import java.util.EnumSet;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags.ConditionalOverwrite;
import static org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags.ConditionalOverwriteEtag;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Extensible structure for options when putting/writing objects.
 */
public final class PutObjectOptions {

  /**
   * Can the PUT operation skip marker deletion?
   */
  private final boolean keepMarkers;

  /**
   * Storage class, if not null.
   */
  private final String storageClass;

  /**
   * Headers; may be null.
   */
  private final Map<String, String> headers;

  /**
   * Flags to control the write process.
   */
  private final EnumSet<WriteObjectFlags> writeObjectFlags;

  /**
   * If set, allows overwriting an object only if the object's ETag matches this value.
   */
  private final String etagOverwrite;

  /**
   * Constructor.
   * @param keepMarkers Can the PUT operation skip marker deletion?
   * @param storageClass Storage class, if not null.
   * @param headers Headers; may be null.
   * @param writeObjectFlags flags for writing
   * @param etagOverwrite etag for etag writes.
   *                      MUST not be empty if etag overwrite flag is set.
   */
  public PutObjectOptions(
      final boolean keepMarkers,
      @Nullable final String storageClass,
      @Nullable final Map<String, String> headers,
      final EnumSet<WriteObjectFlags> writeObjectFlags,
      @Nullable final String etagOverwrite) {
    this.keepMarkers = keepMarkers;
    this.storageClass = storageClass;
    this.headers = headers;
    this.writeObjectFlags = writeObjectFlags;
    this.etagOverwrite = etagOverwrite;
    if (isEtagOverwrite()) {
      checkArgument(!isEmpty(etagOverwrite),
          "etag overwrite is enabled but the etag string is null/empty");
    }
  }

  /**
   * Get the noObjectOverwrite flag.
   * @return true if object override not allowed.
   */
  public boolean isNoObjectOverwrite() {
    return hasFlag(ConditionalOverwrite);
  }

  /**
   * Get the isEtagOverwrite flag.
   * @return true if the write MUST overwrite an object with the
   * supplied etag.
   */
  public boolean isEtagOverwrite() {
    return hasFlag(ConditionalOverwriteEtag);
  }

  /**
   * Does the flag set contain the specific flag.
   * @param flag flag to look for
   * @return true if the flag is set.
   */
  public boolean hasFlag(WriteObjectFlags flag) {
    return writeObjectFlags.contains(flag);
  }

  /**
   * Get the ETag that must match for an overwrite operation to proceed.
   * @return The ETag required for overwrite, or {@code null} if no ETag match is required.
   */
  public String getEtagOverwrite() {
    return etagOverwrite;
  }

  /**
   * Get the marker retention flag.
   * @return true if markers are to be retained.
   */
  public boolean isKeepMarkers() {
    return keepMarkers;
  }

  /**
   * Headers for the put/post request.
   * @return headers or null.
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  public EnumSet<WriteObjectFlags> getWriteObjectFlags() {
    return writeObjectFlags;
  }

  @Override
  public String toString() {
    return "PutObjectOptions{" +
        "keepMarkers=" + keepMarkers +
        ", storageClass='" + storageClass + '\'' +
        ", headers=" + headers +
        ", writeObjectFlags=" + writeObjectFlags +
        ", etagOverwrite='" + etagOverwrite + '\'' +
        '}';
  }
  /**
   * Empty options.
   */
  private static final PutObjectOptions EMPTY_OPTIONS = new PutObjectOptions(
      true,
      null,
      null,
      EnumSet.noneOf(WriteObjectFlags.class),
      null);
  
  private static final PutObjectOptions KEEP_DIRS = EMPTY_OPTIONS;

  private static final PutObjectOptions DELETE_DIRS = new PutObjectOptions(
      false,
      null,
      null,
      EnumSet.noneOf(WriteObjectFlags.class),
      null);
  
  
  /**
   * Get the options to keep directories.
   * @return an instance which keeps dirs
   */
  public static PutObjectOptions keepingDirs() {
    return KEEP_DIRS;
  }

  /**
   * Get the options to delete directory markers.
   * @return an instance which deletes dirs
   */
  public static PutObjectOptions deletingDirs() {
    return DELETE_DIRS;
  }

  /**
   * Get the default options.
   * @return an instance with no storage class or headers.
   */
  public static PutObjectOptions defaultOptions() {
    return keepingDirs();
  }

}