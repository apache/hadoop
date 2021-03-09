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

package org.apache.hadoop.fs.s3a.commit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.s3a.Constants.XA_HEADER_PREFIX;
import static org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory.COMMITTER_FACTORY_SCHEME_PATTERN;

/**
 * Constants for working with committers.
 */
@SuppressWarnings("unused")
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class CommitConstants {

  private CommitConstants() {
  }

  /**
   * Path for "magic" writes: path and {@link #PENDING_SUFFIX} files:
   * {@value}.
   */
  public static final String MAGIC = "__magic";

  /**
   * Marker of the start of a directory tree for calculating
   * the final path names: {@value}.
   */
  public static final String BASE = "__base";

  /**
   * Suffix applied to pending commit metadata: {@value}.
   */
  public static final String PENDING_SUFFIX = ".pending";

  /**
   * Suffix applied to multiple pending commit metadata: {@value}.
   */
  public static final String PENDINGSET_SUFFIX = ".pendingset";

  /**
   * Flag to indicate whether support for the Magic committer is enabled
   * in the filesystem.
   * Value: {@value}.
   */
  public static final String MAGIC_COMMITTER_PREFIX
      = "fs.s3a.committer.magic";

  /**
   * Flag to indicate whether support for the Magic committer is enabled
   * in the filesystem.
   * Value: {@value}.
   */
  public static final String MAGIC_COMMITTER_ENABLED
      = MAGIC_COMMITTER_PREFIX + ".enabled";

  /**
   * Flag to indicate whether a stream is a magic output stream;
   * returned in {@code StreamCapabilities}
   * Value: {@value}.
   */
  public static final String STREAM_CAPABILITY_MAGIC_OUTPUT
      = "fs.s3a.capability.magic.output.stream";

  /**
   * Flag to indicate that a store supports magic committers.
   * returned in {@code PathCapabilities}
   * Value: {@value}.
   */
  public static final String STORE_CAPABILITY_MAGIC_COMMITTER
      = "fs.s3a.capability.magic.committer";

  /**
   * Flag to indicate whether a stream is a magic output stream;
   * returned in {@code StreamCapabilities}
   * Value: {@value}.
   */
  @Deprecated
  public static final String STREAM_CAPABILITY_MAGIC_OUTPUT_OLD
      = "s3a:magic.output.stream";

  /**
   * Flag to indicate that a store supports magic committers.
   * returned in {@code PathCapabilities}
   * Value: {@value}.
   */
  @Deprecated
  public static final String STORE_CAPABILITY_MAGIC_COMMITTER_OLD
      = "s3a:magic.committer";

  /**
   * Is the committer enabled by default: {@value}.
   */
  public static final boolean DEFAULT_MAGIC_COMMITTER_ENABLED = true;

  /**
   * This is the "Pending" directory of the {@code FileOutputCommitter};
   * data written here is, in that algorithm, renamed into place.
   * Value: {@value}.
   */
  public static final String TEMPORARY = "_temporary";

  /**
   * Temp data which is not auto-committed: {@value}.
   * Uses a different name from normal just to make clear it is different.
   */
  public static final String TEMP_DATA = "__temp-data";


  /**
   * Flag to trigger creation of a marker file on job completion.
   */
  public static final String CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER
      = "mapreduce.fileoutputcommitter.marksuccessfuljobs";

  /**
   * Marker file to create on success: {@value}.
   */
  public static final String _SUCCESS = "_SUCCESS";

  /** Default job marker option: {@value}. */
  public static final boolean DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER = true;

  /**
   * Key to set for the S3A schema to use the specific committer.
   */
  public static final String S3A_COMMITTER_FACTORY_KEY = String.format(
      COMMITTER_FACTORY_SCHEME_PATTERN, "s3a");

  /**
   * S3 Committer factory: {@value}.
   * This uses the value of {@link #FS_S3A_COMMITTER_NAME}
   * to choose the final committer.
   */
  public static final String S3A_COMMITTER_FACTORY =
      S3ACommitterFactory.CLASSNAME;

  /**
   * Option to identify the S3A committer:
   * {@value}.
   */
  public static final String FS_S3A_COMMITTER_NAME =
      "fs.s3a.committer.name";

  /**
   * Option for {@link #FS_S3A_COMMITTER_NAME}:
   * classic/file output committer: {@value}.
   */
  public static final String COMMITTER_NAME_FILE = "file";

  /**
   * Option for {@link #FS_S3A_COMMITTER_NAME}:
   * magic output committer: {@value}.
   */
  public static final String COMMITTER_NAME_MAGIC = "magic";

  /**
   * Option for {@link #FS_S3A_COMMITTER_NAME}:
   * directory output committer: {@value}.
   */
  public static final String COMMITTER_NAME_DIRECTORY = "directory";

  /**
   * Option for {@link #FS_S3A_COMMITTER_NAME}:
   * partition output committer: {@value}.
   */
  public static final String COMMITTER_NAME_PARTITIONED = "partitioned";

  /**
   * Option for final files to have a uniqueness name through job attempt info,
   * falling back to a new UUID if there is no job attempt information to use.
   * {@value}.
   * When writing data with the "append" conflict option, this guarantees
   * that new data will not overwrite any existing data.
   */
  public static final String FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES =
      "fs.s3a.committer.staging.unique-filenames";
  /**
   * Default value for {@link #FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES}:
   * {@value}.
   */
  public static final boolean DEFAULT_STAGING_COMMITTER_UNIQUE_FILENAMES = true;

  /**
   * Staging committer conflict resolution policy: {@value}.
   * Supported: fail, append, replace.
   */
  public static final String FS_S3A_COMMITTER_STAGING_CONFLICT_MODE =
      "fs.s3a.committer.staging.conflict-mode";

  /** Conflict mode: {@value}. */
  public static final String CONFLICT_MODE_FAIL = "fail";

  /** Conflict mode: {@value}. */
  public static final String CONFLICT_MODE_APPEND = "append";

  /** Conflict mode: {@value}. */
  public static final String CONFLICT_MODE_REPLACE = "replace";

  /** Default conflict mode: {@value}. */
  public static final String DEFAULT_CONFLICT_MODE = CONFLICT_MODE_APPEND;

  /**
   * Number of threads in committers for parallel operations on files
   * (upload, commit, abort, delete...): {@value}.
   */
  public static final String FS_S3A_COMMITTER_THREADS =
      "fs.s3a.committer.threads";
  /**
   * Default value for {@link #FS_S3A_COMMITTER_THREADS}: {@value}.
   */
  public static final int DEFAULT_COMMITTER_THREADS = 8;

  /**
   * Path  in the cluster filesystem for temporary data: {@value}.
   * This is for HDFS, not the local filesystem.
   * It is only for the summary data of each file, not the actual
   * data being committed.
   */
  public static final String FS_S3A_COMMITTER_STAGING_TMP_PATH =
      "fs.s3a.committer.staging.tmp.path";


  /**
   * Should committers abort all pending uploads to the destination
   * directory?
   * <p>
   * Deprecated: switch to {@link #FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS}.
   */
  @Deprecated
  public static final String FS_S3A_COMMITTER_STAGING_ABORT_PENDING_UPLOADS =
      "fs.s3a.committer.staging.abort.pending.uploads";

  /**
   * Should committers abort all pending uploads to the destination
   * directory?
   * <p>
   * Value: {@value}.
   * <p>
   * Change this is if more than one committer is
   * writing to the same destination tree simultaneously; otherwise
   * the first job to complete will cancel all outstanding uploads from the
   * others. If disabled, configure the bucket lifecycle to remove uploads
   * after a time period, and/or set up a workflow to explicitly delete
   * entries. Otherwise there is a risk that uncommitted uploads may run up
   * bills.
   */
  public static final String FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS =
      "fs.s3a.committer.abort.pending.uploads";

  /**
   * Default configuration value for
   * {@link #FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS}.
   * Value: {@value}.
   */
  public static final boolean DEFAULT_FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS =
      true;

  /**
   * The limit to the number of committed objects tracked during
   * job commits and saved to the _SUCCESS file.
   */
  public static final int SUCCESS_MARKER_FILE_LIMIT = 100;

  /** Extra Data key for task attempt in pendingset files. */
  public static final String TASK_ATTEMPT_ID = "task.attempt.id";

  /**
   * Require the spark UUID to be passed down: {@value}.
   * This is to verify that SPARK-33230 has been applied to spark, and that
   * {@link InternalCommitterConstants#SPARK_WRITE_UUID} is set.
   * <p>
   *   MUST ONLY BE SET WITH SPARK JOBS.
   * </p>
   */
  public static final String FS_S3A_COMMITTER_REQUIRE_UUID =
      "fs.s3a.committer.require.uuid";

  /**
   * Default value for {@link #FS_S3A_COMMITTER_REQUIRE_UUID}: {@value}.
   */
  public static final boolean DEFAULT_S3A_COMMITTER_REQUIRE_UUID =
      false;

  /**
   * Generate a UUID in job setup rather than fall back to
   * YARN Application attempt ID.
   * <p>
   *   MUST ONLY BE SET WITH SPARK JOBS.
   * </p>
   */
  public static final String FS_S3A_COMMITTER_GENERATE_UUID =
      "fs.s3a.committer.generate.uuid";

  /**
   * Default value for {@link #FS_S3A_COMMITTER_GENERATE_UUID}: {@value}.
   */
  public static final boolean DEFAULT_S3A_COMMITTER_GENERATE_UUID =
      false;

  /**
   * Magic Marker header to declare final file length on magic uploads
   * marker objects: {@value}.
   */
  public static final String X_HEADER_MAGIC_MARKER =
      "x-hadoop-s3a-magic-data-length";

  /**
   * XAttr name of magic marker, with "header." prefix: {@value}.
   */
  public static final String XA_MAGIC_MARKER = XA_HEADER_PREFIX
      + X_HEADER_MAGIC_MARKER;

}
