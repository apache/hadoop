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

package org.apache.hadoop.fs.s3a.performance;

/**
 * Declaration of the costs of head and list calls for various FS IO
 * operations.
 * <p></p>
 * An instance declares the number of head and list calls expected for
 * various operations -with a {@link #plus(OperationCost)}
 * method to add operation costs together to produce an
 * aggregate cost. These can then be validated in tests
 * via {@link OperationCostValidator}.
 *
 */
public final class OperationCost {

  /** Head costs for getFileStatus() directory probe: {@value}. */
  public static final int FILESTATUS_DIR_PROBE_H = 0;

  /** List costs for getFileStatus() directory probe: {@value}. */
  public static final int FILESTATUS_DIR_PROBE_L = 1;

  /** Head cost getFileStatus() file probe only. */
  public static final int FILESTATUS_FILE_PROBE_H = 1;

  /** Liast cost getFileStatus() file probe only. */

  public static final int FILESTATUS_FILE_PROBE_L = 0;

  /**
   * Delete cost when deleting an object.
   */
  public static final int DELETE_OBJECT_REQUEST = 1;

  /**
   * Delete cost when deleting a marker.
   * Note: if bulk delete is disabled, this changes to being
   * the number of directories deleted.
   */
  public static final int DELETE_MARKER_REQUEST = DELETE_OBJECT_REQUEST;

  /**
   * No IO takes place.
   */
  public static final OperationCost NO_IO =
      new OperationCost(0, 0);

  /** A HEAD operation. */
  public static final OperationCost HEAD_OPERATION = new OperationCost(1, 0);

  /** A LIST operation. */
  public static final OperationCost LIST_OPERATION = new OperationCost(0, 1);

  /**
   * Cost of {@link org.apache.hadoop.fs.s3a.impl.StatusProbeEnum#DIRECTORIES}.
   */
  public static final OperationCost FILE_STATUS_DIR_PROBE = LIST_OPERATION;

  /**
   * Cost of {@link org.apache.hadoop.fs.s3a.impl.StatusProbeEnum#FILE}.
   */
  public static final OperationCost FILE_STATUS_FILE_PROBE = HEAD_OPERATION;

  /**
   * Cost of getFileStatus on root directory.
   */
  public static final OperationCost ROOT_FILE_STATUS_PROBE = NO_IO;

  /**
   * Cost of {@link org.apache.hadoop.fs.s3a.impl.StatusProbeEnum#ALL}.
   */
  public static final OperationCost FILE_STATUS_ALL_PROBES =
      FILE_STATUS_FILE_PROBE.plus(FILE_STATUS_DIR_PROBE);

  /** getFileStatus() on a file which exists. */
  public static final OperationCost GET_FILE_STATUS_ON_FILE =
      FILE_STATUS_FILE_PROBE;

  /** List costs for getFileStatus() on a non-empty directory: {@value}. */
  public static final OperationCost GET_FILE_STATUS_ON_DIR =
      FILE_STATUS_FILE_PROBE.plus(FILE_STATUS_DIR_PROBE);

  /** Costs for getFileStatus() on an empty directory: {@value}. */
  public static final OperationCost GET_FILE_STATUS_ON_EMPTY_DIR =
      GET_FILE_STATUS_ON_DIR;

  /** getFileStatus() directory marker which exists. */
  public static final OperationCost GET_FILE_STATUS_ON_DIR_MARKER =
      GET_FILE_STATUS_ON_EMPTY_DIR;

  /** getFileStatus() call which fails to find any entry. */
  public static final OperationCost GET_FILE_STATUS_FNFE =
      FILE_STATUS_ALL_PROBES;

  /** listLocatedStatus always does a LIST. */
  public static final OperationCost LIST_LOCATED_STATUS_LIST_OP =
      new OperationCost(0, 1);

  /** listFiles always does a LIST. */
  public static final OperationCost LIST_FILES_LIST_OP = LIST_OPERATION;

  /** listStatus always does a LIST. */
  public static final OperationCost LIST_STATUS_LIST_OP = LIST_OPERATION;
  /**
   * Metadata cost of a copy operation, as used during rename.
   * This happens even if the store is guarded.
   */
  public static final OperationCost COPY_OP =
      new OperationCost(1, 0);

  /**
   * Cost of renaming a file to a different directory.
   * <p></p>
   * LIST on dest not found, look for dest dir, and then, at
   * end of rename, whether a parent dir needs to be created.
   */
  public static final OperationCost RENAME_SINGLE_FILE_DIFFERENT_DIR =
      FILE_STATUS_FILE_PROBE              // source file probe
          .plus(GET_FILE_STATUS_FNFE)     // dest does not exist
          .plus(FILE_STATUS_FILE_PROBE)   // parent dir of dest is not file
          .plus(FILE_STATUS_DIR_PROBE)    // recreate source parent dir?
          .plus(COPY_OP);                 // metadata read on copy

  /**
   * Cost of renaming a file to the same directory
   * <p></p>
   * No need to look for parent directories, so only file
   * existence checks and the copy.
   */
  public static final OperationCost RENAME_SINGLE_FILE_SAME_DIR =
      FILE_STATUS_FILE_PROBE              // source file probe
          .plus(GET_FILE_STATUS_FNFE)     // dest must not exist
          .plus(COPY_OP);                 // metadata read on copy

  /**
   * create(overwrite = true) does not look for the file existing.
   */
  public static final OperationCost CREATE_FILE_OVERWRITE =
      FILE_STATUS_DIR_PROBE;

  /**
   * create(overwrite = false) runs all the checks.
   */
  public static final OperationCost CREATE_FILE_NO_OVERWRITE =
      FILE_STATUS_ALL_PROBES;

  /**
   * S3Guard in non-auth mode always attempts a single file
   * status call.
   */
  public static final OperationCost S3GUARD_NONAUTH_FILE_STATUS_PROBE =
      FILE_STATUS_FILE_PROBE;

  /** Expected HEAD count. */
  private final int head;

  /** Expected LIST count. */
  private final int list;

  /**
   * Constructor.
   * @param head head requests.
   * @param list list requests.
   */
  public OperationCost(final int head,
      final int list) {
    this.head = head;
    this.list = list;
  }

  /** Expected HEAD count. */
  int head() {
    return head;
  }

  /** Expected LIST count. */
  int list() {
    return list;
  }

  /**
   * Add to create a new cost.
   * @param that the other entry
   * @return cost of the combined operation.
   */
  public OperationCost plus(OperationCost that) {
    return new OperationCost(
        head + that.head,
        list + that.list);
  }

  @Override
  public String toString() {
    return "OperationCost{" +
        "head=" + head +
        ", list=" + list +
        '}';
  }
}
