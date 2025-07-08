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

package org.apache.hadoop.fs.gs;

import java.time.Duration;
import java.util.regex.Pattern;

import static java.lang.Math.toIntExact;

import org.apache.hadoop.conf.Configuration;

/**
 * This class provides a configuration for the {@link GoogleHadoopFileSystem} implementations.
 */
class GoogleHadoopFileSystemConfiguration {
  private static final Long GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT_DEFAULT = 8 * 1024 * 1024L;

  /**
   * Configuration key for default block size of a file.
   *
   * <p>Note that this is the size that is reported to Hadoop FS clients. It does not modify the
   * actual block size of an underlying GCS object, because GCS JSON API does not allow modifying or
   * querying the value. Modifying this value allows one to control how many mappers are used to
   * process a given file.
   */
  static final HadoopConfigurationProperty<Long> BLOCK_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.block.size", 64 * 1024 * 1024L);

  /**
   * Configuration key for GCS project ID. Default value: none
   */
  private static final HadoopConfigurationProperty<String> GCS_PROJECT_ID =
      new HadoopConfigurationProperty<>("fs.gs.project.id");

  /**
   * Configuration key for initial working directory of a GHFS instance. Default value: '/'
   */
  static final HadoopConfigurationProperty<String> GCS_WORKING_DIRECTORY =
      new HadoopConfigurationProperty<>("fs.gs.working.dir", "/");

  /**
   * Configuration key for setting write buffer size.
   */
  private static final HadoopConfigurationProperty<Long> GCS_OUTPUT_STREAM_BUFFER_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.buffer.size", 8L * 1024 * 1024);


  /**
   * If forward seeks are within this many bytes of the current position, seeks are performed by
   * reading and discarding bytes in-place rather than opening a new underlying stream.
   */
  private static final HadoopConfigurationProperty<Long> GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.inplace.seek.limit",
          GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT_DEFAULT);

  /** Tunes reading objects behavior to optimize HTTP GET requests for various use cases. */
  private static final HadoopConfigurationProperty<Fadvise> GCS_INPUT_STREAM_FADVISE =
      new HadoopConfigurationProperty<>("fs.gs.inputstream.fadvise", Fadvise.RANDOM);

  /**
   * If false, reading a file with GZIP content encoding (HTTP header "Content-Encoding: gzip") will
   * result in failure (IOException is thrown).
   */
  private static final HadoopConfigurationProperty<Boolean>
      GCS_INPUT_STREAM_SUPPORT_GZIP_ENCODING_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.support.gzip.encoding.enable",
          false);

  /**
   * Minimum size in bytes of the HTTP Range header set in GCS request when opening new stream to
   * read an object.
   */
  private static final HadoopConfigurationProperty<Long> GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.min.range.request.size",
          2 * 1024 * 1024L);

  /**
   * Configuration key for number of request to track for adapting the access pattern i.e. fadvise:
   * AUTO & AUTO_RANDOM.
   */
  private static final HadoopConfigurationProperty<Integer> GCS_FADVISE_REQUEST_TRACK_COUNT =
      new HadoopConfigurationProperty<>("fs.gs.fadvise.request.track.count", 3);

  /**
   * Configuration key for specifying max number of bytes rewritten in a single rewrite request when
   * fs.gs.copy.with.rewrite.enable is set to 'true'.
   */
  private static final HadoopConfigurationProperty<Long> GCS_REWRITE_MAX_CHUNK_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.rewrite.max.chunk.size",
          512 * 1024 * 1024L);

  /** Configuration key for marker file pattern. Default value: none */
  private static final HadoopConfigurationProperty<String> GCS_MARKER_FILE_PATTERN =
      new HadoopConfigurationProperty<>("fs.gs.marker.file.pattern");

  /**
   * Configuration key for enabling check to ensure that conflicting directories do not exist when
   * creating files and conflicting files do not exist when creating directories.
   */
  private static final HadoopConfigurationProperty<Boolean> GCS_CREATE_ITEMS_CONFLICT_CHECK_ENABLE =
      new HadoopConfigurationProperty<>(
          "fs.gs.create.items.conflict.check.enable",
          true);

  /**
   * Configuration key for the minimal time interval between consecutive sync/hsync/hflush calls.
   */
  private static final HadoopConfigurationProperty<Long> GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.sync.min.interval",
          0L);

  /**
   * If true, recursive delete on a path that refers to a GCS bucket itself ('/' for any
   * bucket-rooted GoogleHadoopFileSystem) or delete on that path when it's empty will result in
   * fully deleting the GCS bucket. If false, any operation that normally would have deleted the
   * bucket will be ignored instead. Setting to 'false' preserves the typical behavior of "rm -rf /"
   * which translates to deleting everything inside of root, but without clobbering the filesystem
   * authority corresponding to that root path in the process.
   */
  static final HadoopConfigurationProperty<Boolean> GCE_BUCKET_DELETE_ENABLE =
          new HadoopConfigurationProperty<>(
                  "fs.gs.bucket.delete.enable",
                  false);

  private final String workingDirectory;
  private final String projectId;
  private final Configuration config;
  private Pattern fileMarkerFilePattern;

  int getOutStreamBufferSize() {
    return outStreamBufferSize;
  }

  private final int outStreamBufferSize;

  GoogleHadoopFileSystemConfiguration(Configuration conf) {
    this.workingDirectory = GCS_WORKING_DIRECTORY.get(conf, conf::get);
    this.outStreamBufferSize =
        toIntExact(GCS_OUTPUT_STREAM_BUFFER_SIZE.get(conf, conf::getLongBytes));
    this.projectId = GCS_PROJECT_ID.get(conf, conf::get);
    this.config = conf;
  }

  String getWorkingDirectory() {
    return this.workingDirectory;
  }

  String getProjectId() {
    return this.projectId;
  }

  long getMaxListItemsPerCall() {
    return 5000L; //TODO: Make this configurable
  }

  Fadvise getFadvise() {
    return GCS_INPUT_STREAM_FADVISE.get(config, config::getEnum);
  }

  long getInplaceSeekLimit() {
    return GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.get(config, config::getLongBytes);
  }

  int getFadviseRequestTrackCount() {
    return GCS_FADVISE_REQUEST_TRACK_COUNT.get(config, config::getInt);
  }

  boolean isGzipEncodingSupportEnabled() {
    return GCS_INPUT_STREAM_SUPPORT_GZIP_ENCODING_ENABLE.get(config, config::getBoolean);
  }

  long getMinRangeRequestSize() {
    return GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.get(config, config::getLongBytes);
  }

  long getBlockSize() {
    return BLOCK_SIZE.get(config, config::getLong);
  }

  boolean isReadExactRequestedBytesEnabled() {
    return false; //TODO: Remove this option?
  }

  long getMaxRewriteChunkSize() {
    return GCS_REWRITE_MAX_CHUNK_SIZE.get(config, config::getLong);
  }

  Pattern getMarkerFilePattern() {
    String pattern =  GCS_MARKER_FILE_PATTERN.get(config, config::get);
    if (pattern == null) {
      return null;
    }

    if (fileMarkerFilePattern == null) {
      // Caching the pattern since compile step can be expensive
      fileMarkerFilePattern =  Pattern.compile("^(.+/)?" + pattern + "$");
    }

    return fileMarkerFilePattern;
  }

  boolean isEnsureNoConflictingItems() {
    return GCS_CREATE_ITEMS_CONFLICT_CHECK_ENABLE.get(config, config::getBoolean);
  }

  Duration getMinSyncInterval() {
    return GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL.getTimeDuration(config);
  }

  Configuration getConfig() {
    return config;
  }

  boolean isBucketDeleteEnabled() {
    return GCE_BUCKET_DELETE_ENABLE.get(config, config::getBoolean);
  }
}
