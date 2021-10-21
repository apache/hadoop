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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_DIRECTORY;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_FILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_LIST_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.*;
/**
 * Constants internal and external for the manifest committer.
 */

@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ManifestCommitterConstants {

  /**
   * Suffix to use in manifest files in the job attempt dir.
   * Value: {@value}.
   */
  public static final String MANIFEST_SUFFIX = "-manifest.json";

  /**
   * Prefix for summary files in the report dir. Call
   */
  public static final String SUMMARY_FILENAME_PREFIX = "summary-";

  /**
   * Format string used to build a summary file from a Job ID.
   */
  public static final String SUMMARY_FILENAME_FORMAT =
      SUMMARY_FILENAME_PREFIX + "%s.json";

  /**
   * Suffix to use for temp files before renaming them.
   * Value: {@value}.
   */
  public static final String TMP_SUFFIX = ".tmp";

  /**
   * Initial number of all app attempts.
   * This is fixed in YARN; for Spark jobs the
   * same number "0" is used.
   */
  public static final int INITIAL_APP_ATTEMPT_ID = 0;

  /**
   * Format string for building a job dir.
   * Value: {@value}.
   */
  public static final String JOB_DIR_FORMAT_STR = "manifest_%s";

  /**
   * Format string for building a job attempt dir.
   * This uses the job attempt number so previous versions
   * can be found trivially.
   * Value: {@value}.
   */
  public static final String JOB_ATTEMPT_DIR_FORMAT_STR = "%d";

  /**
   * Committer classname as recorded in the committer _SUCCESS file.
   */
  public static final String MANIFEST_COMMITTER_CLASSNAME =
      "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter";

  /**
   * Marker file to create on success: {@value}.
   */
  public static final String SUCCESS_MARKER = "_SUCCESS";

  /** Default job marker option: {@value}. */
  public static final boolean DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER = true;


  /**
   * The limit to the number of committed objects tracked during
   * job commits and saved to the _SUCCESS file.
   */
  public static final int SUCCESS_MARKER_FILE_LIMIT = 100;

  /**
   * The UUID for jobs: {@value}.
   * This was historically created in Spark 1.x's SQL queries,
   * but "went away".
   * It has been restored in recent spark releases.
   * If found: it is used instead of the MR job attempt ID.
   */
  public static final String SPARK_WRITE_UUID = "spark.sql.sources.writeJobUUID";

  /**
   * String to use as source of the job ID.
   * This SHOULD be kept in sync with that of
   * {@code AbstractS3ACommitter.JobUUIDSource}.
   */
  public static final String JOB_ID_SOURCE_MAPREDUCE = "JobID";

  /**
   * Prefix to use for config options: {@value }.
   */
  public static final String OPT_PREFIX = "mapreduce.manifest.committer.";

  /**
   * rather than delete in cleanup, should the working directory
   * be moved to the trash directory?
   * Potentially faster on some stores.
   */
  public static final String OPT_CLEANUP_MOVE_TO_TRASH =
      OPT_PREFIX +"cleanup.move.to.trash";

  /**
   * Default value:  {@value }.
   */
  public static final boolean OPT_CLEANUP_MOVE_TO_TRASH_DEFAULT = false;

  /**
   * Should dir cleanup do parallel deletion of task attempt dirs
   * before trying to delete the toplevel dirs.
   * For GCS this may deliver speedup, while on ABFS it may avoid
   * timeouts in certain deployments.
   */
  public static final String OPT_CLEANUP_PARALLEL_ATTEMPT_DIRS =
      OPT_PREFIX +"cleanup.parallel.delete.attempt.directories";

  /**
   * Default value:  {@value}.
   */
  public static final boolean OPT_CLEANUP_PARALLEL_ATTEMPT_DIRS_DEFAULT = true;

  /**
   * Threads to use for IO.
   */
  public static final String OPT_IO_PROCESSORS = OPT_PREFIX + "io.thread.count";

  /**
   * Default value:  {@value }.
   */
  public static final int OPT_IO_PROCESSORS_DEFAULT = 64;

  /**
   * Rate limit in operations/second for read operations.
   */
  public static final String OPT_IO_READ_RATE = OPT_PREFIX + "io.read.rate";

  /**
   * Default value:  {@value }.
   */
  public static final int OPT_IO_READ_RATE_DEFAULT = 10000;

  /**
   * Rate limit in operations/second for write operations.
   */
  public static final String OPT_IO_WRITE_RATE = OPT_PREFIX + "io.write.rate";

  /**
   * Default value:  {@value }.
   */
  public static final int OPT_IO_WRITE_RATE_DEFAULT = 10000;


  /**
   * Directory for saving job summary reports.
   * These are the _SUCCESS files, but are saved even on
   * job failures.
   */
  public static final String OPT_SUMMARY_REPORT_DIR =
      OPT_PREFIX + "summary.report.directory";

  /**
   * Should the output be validated?
   */
  public static final String OPT_VALIDATE_OUTPUT = OPT_PREFIX + "validate.output";

  /**
   * Default value:  {@value }.
   */
  public static final boolean OPT_VALIDATE_OUTPUT_DEFAULT = false;

  /**
   * Name of the factory: {@value}.
   */
  public static final String MANIFEST_COMMITTER_FACTORY =
      "org.apache.hadoop.mapreduce.lib.output.committer.manifest" +
          ".ManifestCommitterFactory";

  /**
   * Stage attribute in audit context: {@value}.
   */
  public static final String CONTEXT_ATTR_STAGE = "st";

  /**
   * Task ID attribute in audit context: {@value}.
   */
  public static final String CONTEXT_ATTR_TASK_ATTEMPT_ID = "ta";

  private ManifestCommitterConstants() {
  }

}
