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
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperationsThroughFileSystem;

/**
 * Public constants for the manifest committer.
 * This includes all configuration options and their default values.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ManifestCommitterConstants {

  /**
   * Suffix to use in manifest files in the manifest subdir.
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
  public static final String JOB_DIR_FORMAT_STR = "%s";

  /**
   * Format string for building a job attempt dir.
   * This uses the job attempt number so previous versions
   * can be found trivially.
   * Value: {@value}.
   */
  public static final String JOB_ATTEMPT_DIR_FORMAT_STR = "%02d";

  /**
   * Name of directory under job attempt dir for manifests.
   */
  public static final String JOB_TASK_MANIFEST_SUBDIR = "manifests";

  /**
   * Name of directory under job attempt dir for task attempts.
   */
  public static final String JOB_TASK_ATTEMPT_SUBDIR = "tasks";


  /**
   * Committer classname as recorded in the committer _SUCCESS file.
   */
  public static final String MANIFEST_COMMITTER_CLASSNAME =
      ManifestCommitter.class.getName();

  /**
   * Marker file to create on success: {@value}.
   */
  public static final String SUCCESS_MARKER = "_SUCCESS";

  /** Default job marker option: {@value}. */
  public static final boolean DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER = true;

  /**
   * The limit to the number of committed objects tracked during
   * job commits and saved to the _SUCCESS file.
   * Value: {@value}.
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
   * Value: {@value}.
   */
  public static final String JOB_ID_SOURCE_MAPREDUCE = "JobID";

  /**
   * Prefix to use for config options: {@value}.
   */
  public static final String OPT_PREFIX = "mapreduce.manifest.committer.";

  /**
   * Should dir cleanup do parallel deletion of task attempt dirs
   * before trying to delete the toplevel dirs.
   * For GCS this may deliver speedup, while on ABFS it may avoid
   * timeouts in certain deployments.
   * Value: {@value}.
   */
  public static final String OPT_CLEANUP_PARALLEL_DELETE =
      OPT_PREFIX + "cleanup.parallel.delete";

  /**
   * Default value:  {@value}.
   */
  public static final boolean OPT_CLEANUP_PARALLEL_DELETE_DIRS_DEFAULT = true;

  /**
   * Threads to use for IO.
   */
  public static final String OPT_IO_PROCESSORS = OPT_PREFIX + "io.threads";

  /**
   * Default value:  {@value}.
   */
  public static final int OPT_IO_PROCESSORS_DEFAULT = 32;

  /**
   * Directory for saving job summary reports.
   * These are the _SUCCESS files, but are saved even on
   * job failures.
   * Value: {@value}.
   */
  public static final String OPT_SUMMARY_REPORT_DIR =
      OPT_PREFIX + "summary.report.directory";

  /**
   * Directory for moving manifests under for diagnostics.
   * Value: {@value}.
   */
  public static final String OPT_DIAGNOSTICS_MANIFEST_DIR =
      OPT_PREFIX + "diagnostics.manifest.directory";

  /**
   * Should the output be validated?
   * This will check expected vs actual file lengths, and,
   * if etags can be obtained, etags.
   * Value: {@value}.
   */
  public static final String OPT_VALIDATE_OUTPUT = OPT_PREFIX + "validate.output";

  /**
   * Default value: {@value}.
   */
  public static final boolean OPT_VALIDATE_OUTPUT_DEFAULT = false;

  /**
   * Should job commit delete for files/directories at the targets
   * of renames, and, if found, deleting them?
   *
   * This is part of the effective behavior of the FileOutputCommitter,
   * however it adds an extra delete call per file being committed.
   *
   * If a job is writing to a directory which has only just been created
   * or were unique filenames are being used, there is no need to perform
   * this preparation.
   * The recognition of newly created dirs is automatic.
   *
   * Value: {@value}.
   */
  public static final String OPT_DELETE_TARGET_FILES =
      OPT_PREFIX + "delete.target.files";

  /**
   * Default value: {@value}.
   */
  public static final boolean OPT_DELETE_TARGET_FILES_DEFAULT = false;

  /**
   * Name of the factory: {@value}.
   */
  public static final String MANIFEST_COMMITTER_FACTORY =
      ManifestCommitterFactory.class.getName();

  /**
   * Classname of the store operations; filesystems and tests
   * may override.
   * Value: {@value}.
   */
  public static final String OPT_STORE_OPERATIONS_CLASS = OPT_PREFIX + "store.operations.classname";

  /**
   * Default classname of the store operations.
   * Value: {@value}.
   */
  public static final String STORE_OPERATIONS_CLASS_DEFAULT =
      ManifestStoreOperationsThroughFileSystem.class.getName();

  /**
   * Stage attribute in audit context: {@value}.
   */
  public static final String CONTEXT_ATTR_STAGE = "st";

  /**
   * Task ID attribute in audit context: {@value}.
   */
  public static final String CONTEXT_ATTR_TASK_ATTEMPT_ID = "ta";

  /**
   * Stream Capabilities probe for spark dynamic partitioning compatibility.
   */
  public static final String CAPABILITY_DYNAMIC_PARTITIONING =
      "mapreduce.job.committer.dynamic.partitioning";


  /**
   * Queue capacity between task manifest loading an entry file writer.
   * If more than this number of manifest lists are waiting to be written,
   * the enqueue is blocking.
   * There's an expectation that writing to the local file is a lot faster
   * than the parallelized buffer reads, therefore that this queue can
   * be emptied at the same rate it is filled.
   * Value {@value}.
   */
  public static final String OPT_WRITER_QUEUE_CAPACITY =
      OPT_PREFIX + "writer.queue.capacity";


  /**
   * Default value of {@link #OPT_WRITER_QUEUE_CAPACITY}.
   * Value {@value}.
   */
  public static final int DEFAULT_WRITER_QUEUE_CAPACITY = OPT_IO_PROCESSORS_DEFAULT;

  private ManifestCommitterConstants() {
  }

}
