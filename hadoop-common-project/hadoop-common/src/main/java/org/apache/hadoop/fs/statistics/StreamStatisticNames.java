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

package org.apache.hadoop.fs.statistics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * These are common statistic names.
 * <p>
 * When adding new common statistic name constants, please make them unique.
 * By convention, they are implicitly unique:
 * <ul>
 *   <li>
 *     The name of the constants are uppercase, words separated by
 *     underscores.
 *   </li>
 *   <li>
 *     The value of the constants are lowercase of the constant names.
 *   </li>
 * </ul>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class StreamStatisticNames {

  /** {@value}. */
  public static final String STREAM_READ_ABORTED = "stream_aborted";

  /** {@value}. */
  public static final String STREAM_READ_BYTES_DISCARDED_ABORT
      = "stream_read_bytes_discarded_in_abort";

  /** {@value}. */
  public static final String STREAM_READ_BYTES_DISCARDED_SEEK
      = "stream_read_bytes_discarded_in_seek";

  /** {@value}. */
  public static final String STREAM_READ_CLOSED = "stream_read_closed";

  /** {@value}. */
  public static final String STREAM_READ_CLOSE_BYTES_READ
      = "stream_read_bytes_read_in_close";

  /** {@value}. */
  public static final String STREAM_READ_CLOSE_OPERATIONS
      = "stream_read_close_operations";

  /** {@value}. */
  public static final String STREAM_READ_OPENED = "stream_read_opened";

  /** {@value}. */
  public static final String STREAM_READ_BYTES
      = "stream_read_bytes";

  /** {@value}. */
  public static final String STREAM_READ_EXCEPTIONS =
      "stream_read_exceptions";

  /** {@value}. */
  public static final String STREAM_READ_FULLY_OPERATIONS
      = "stream_read_fully_operations";

  /** {@value}. */
  public static final String STREAM_READ_OPERATIONS =
      "stream_read_operations";

  /** {@value}. */
  public static final String STREAM_READ_OPERATIONS_INCOMPLETE
      = "stream_read_operations_incomplete";

  /** {@value}. */
  public static final String STREAM_READ_VERSION_MISMATCHES
      = "stream_read_version_mismatches";

  /** {@value}. */
  public static final String STREAM_READ_SEEK_BYTES_BACKWARDS
      = "stream_read_bytes_backwards_on_seek";

  /** {@value}. */
  public static final String STREAM_READ_SEEK_FORWARD_OPERATIONS
      = "stream_read_seek_forward_operations";

  /** {@value}. */
  public static final String STREAM_READ_SEEK_POLICY_CHANGED =
      "stream_read_seek_policy_changed";

  /** {@value}. */
  public static final String STREAM_READ_SEEK_OPERATIONS =
      "stream_read_seek_operations";

  /** {@value}. */
  public static final String STREAM_READ_SEEK_BACKWARD_OPERATIONS =
      "stream_read_seek_backward_operations";

  /** {@value}. */
  public static final String STREAM_READ_SEEK_BYTES_READ =
      "stream_read_seek_bytes_read";

  /** {@value}. */
  public static final String STREAM_READ_SEEK_BYTES_SKIPPED
      = "stream_read_bytes_skipped_on_seek";

  /** {@value}. */
  public static final String STREAM_READ_SKIP_OPERATIONS =
      "stream_read_skip_operations";

  /** {@value}. */
  public static final String STREAM_READ_SKIP_BYTES =
      "stream_read_skip_bytes";

  /** {@value}. */
  public static final String STREAM_READ_TOTAL_BYTES
      = "stream_read_total_bytes";

  /** {@value}. */
  public static final String STREAM_WRITE_EXCEPTIONS =
      "stream_write_exceptions";

  /** Failures when finalizing a multipart upload: {@value}. */
  public static final String STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS =
      "stream_write_exceptions_completing_upload";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS
      = "stream_write_block_uploads";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_ABORTED
      = "stream_write_block_uploads_aborted";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_ACTIVE
      = "stream_write_block_uploads_active";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_COMMITTED
      = "stream_write_block_uploads_committed";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_PENDING
      = "stream_write_block_uploads_pending";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING =
      "stream_write_block_uploads_data_pending";

  /**
   * "Count of bytes written to output stream including all not yet uploaded.
   * {@value}.
   */
  public static final String STREAM_WRITE_BYTES
      = "stream_write_bytes";

  /**
   * Count of total time taken for uploads to complete.
   * {@value}.
   */
  public static final String STREAM_WRITE_TOTAL_TIME
      = "stream_write_total_time";

  /**
   * Total queue duration of all block uploads.
   * {@value}.
   */
  public static final String STREAM_WRITE_QUEUE_DURATION
      = "stream_write_queue_duration";

  private StreamStatisticNames() {
  }

}
