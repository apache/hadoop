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
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.PartitonedStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterFactory;

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
      = "s3a:magic.output.stream";

  /**
   * Flag to indicate that a store supports magic committers.
   * returned in {@code StreamCapabilities}
   * Value: {@value}.
   */
  public static final String STORE_CAPABILITY_MAGIC_COMMITTER
      = "s3a:magic.committer";

  /**
   * Is the committer enabled by default? No.
   */
  public static final boolean DEFAULT_MAGIC_COMMITTER_ENABLED = false;

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
   * Directory committer: {@value}.
   */
  public static final String STAGING_COMMITTER_FACTORY =
      StagingCommitterFactory.CLASSNAME;

  /**
   * Directory committer: {@value}.
   */
  public static final String DIRECTORY_COMMITTER_FACTORY =
      DirectoryStagingCommitterFactory.CLASSNAME;

  /**
   * Partitioned committer: {@value}.
   */
  public static final String PARTITION_COMMITTER_FACTORY =
      PartitonedStagingCommitterFactory.CLASSNAME;

  /**
   * Dynamic committer: {@value}.
   */
  public static final String DYNAMIC_COMMITTER_FACTORY =
      DynamicCommitterFactory.CLASSNAME;

  /**
   * Magic committer: {@value}.
   */
  public static final String MAGIC_COMMITTER_FACTORY =
      MagicS3GuardCommitterFactory.CLASSNAME;

  /**
   * Property to identify the S3A committer when the dynamic committer is used:
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
  public static final String COMMITTER_NAME_PARTITION = "partition";

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
   * A unique identifier to use for this work: {@value}.
   */
  public static final String FS_S3A_COMMITTER_STAGING_UUID =
      "fs.s3a.committer.staging.uuid";

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
  public static final String DEFAULT_CONFLICT_MODE = CONFLICT_MODE_FAIL;

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

}
