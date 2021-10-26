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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_DIRECTORY;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_FILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_LIST_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.*;

public class InternalConstants {

  /**
   * Durations.
   */
  public static final String[] DURATION_STATISTICS = {

      /* Job stages. */
      OP_STAGE_JOB_ABORT,
      OP_STAGE_JOB_CLEANUP,
      OP_STAGE_JOB_COMMIT,
      OP_STAGE_JOB_CREATE_TARGET_DIRS,
      OP_STAGE_JOB_LOAD_MANIFESTS,
      OP_STAGE_JOB_RENAME_FILES,
      OP_STAGE_JOB_SAVE_SUCCESS,
      OP_STAGE_JOB_SETUP,
      OP_STAGE_JOB_VALIDATE_OUTPUT,

      /* Task stages. */

      OP_STAGE_TASK_ABORT_TASK,
      OP_STAGE_TASK_COMMIT,
      OP_STAGE_TASK_SAVE_MANIFEST,
      OP_STAGE_TASK_SCAN_DIRECTORY,
      OP_STAGE_TASK_SETUP,


      /* Lower level store/fs operations. */
      OP_COMMIT_FILE_RENAME,
      OP_CREATE_DIRECTORIES,
      OP_CREATE_ONE_DIRECTORY,
      OP_DIRECTORY_SCAN,
      OP_DELETE,
      OP_DELETE_FILE_UNDER_DESTINATION,
      OP_GET_FILE_STATUS,
      OP_IS_DIRECTORY,
      OP_IS_FILE,
      OP_LIST_STATUS,
      OP_LOAD_MANIFEST,
      OP_LOAD_ALL_MANIFESTS,
      OP_MKDIRS,
      OP_MKDIRS_RETURNED_FALSE,
      OP_MSYNC,
      OP_PREPARE_DIR_ANCESTORS,
      OP_RENAME_FILE,
      OP_SAVE_TASK_MANIFEST,

      OBJECT_LIST_REQUEST,
      OBJECT_CONTINUE_LIST_REQUEST,

      IO_ACQUIRE_READ_PERMIT_BLOCKED,
      IO_ACQUIRE_WRITE_PERMIT_BLOCKED
  };

  /**
   * Counters.
   */
  public static final String[] COUNTER_STATISTICS = {
      COMMITTER_BYTES_COMMITTED_COUNT,
      OP_COMMIT_FILE_RENAME_RECOVERED_ETAG_COUNT,
      COMMITTER_FILES_COMMITTED_COUNT,
      COMMITTER_TASKS_COMPLETED_COUNT,
      COMMITTER_TASKS_FAILED_COUNT,
      COMMITTER_TASK_DIRECTORY_COUNT_MEAN,
      COMMITTER_TASK_DIRECTORY_DEPTH_MEAN,
      COMMITTER_TASK_FILE_COUNT_MEAN,
      COMMITTER_TASK_FILE_SIZE_MEAN,
  };

  /**
   * Read permit cost for any of the status probes: {@value}.
   */
  public static final int PERMIT_READ_GET_FILE_STATUS = 1;

  /**
   * Read permit cost for list operations: {@value}.
   */
  public static final int PERMIT_READ_LIST = 2;

  /**
   * Read permit cost for list operations: {@value}.
   */
  public static final int PERMIT_READ_OPEN_FILE = 2;

  /**
   * Write permit cost for delete(): {@value}.
   */
  public static final int PERMIT_WRITE_CREATE_FILE = 1;

  /**
   * Write permit cost for commit(): {@value}.
   */
  public static final int PERMIT_WRITE_COMMIT_FILE = 1;

  /**
   * Write permit cost for delete(): {@value}.
   */
  public static final int PERMIT_WRITE_DELETE = 1;

  /**
   * Write permit cost for mkdir(): {@value}.
   */
  public static final int PERMIT_WRITE_MKDIR = 1;

  /**
   * Write permit cost for rename(): {@value}.
   */
  public static final int PERMIT_WRITE_RENAME = 1;

  /**
   * Error string from ABFS connector on timeout.
   */
  public static final String OPERATION_TIMED_OUT = "OperationTimedOut";

  /**
   * Format string for task attempt names.
   */
  public static final String NAME_FORMAT_TASK_ATTEMPT = "[Task-Attempt %s]";

  /**
   * Format string for job attempt names.
   */
  public static final String NAME_FORMAT_JOB_ATTEMPT = "[Job-Attempt %s]";
}
