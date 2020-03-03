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

import org.apache.hadoop.classification.InterfaceStability;

/**
 * These are common statistic names.
 *
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
@InterfaceStability.Unstable
public class StreamStatisticNames {

  /** {@value}. */
  public static final String STREAM_CLOSED = "stream_closed";

  /** {@value}. */
  public static final String STREAM_CLOSE_OPERATIONS
      = "stream_close_operations";

  /** {@value}. */
  public static final String STREAM_FORWARD_SEEK_OPERATIONS
      = "stream_forward_seek_operations";

  /** {@value}. */
  public static final String STREAM_OPENED = "stream_opened";

  /** {@value}. */
  public static final String STREAM_ABORTED = "stream_aborted";

  /** {@value}. */
  public static final String STREAM_READ_EXCEPTIONS = "stream_read_exceptions";

  /** {@value}. */
  public static final String STREAM_READ_FULLY_OPERATIONS
      = "stream_read_fully_operations";

  /** {@value}. */
  public static final String STREAM_READ_OPERATIONS = "stream_read_operations";

  /** {@value}. */
  public static final String STREAM_READ_OPERATIONS_INCOMPLETE
      = "stream_read_operations_incomplete";

  /** {@value}. */
  public static final String STREAM_READ_VERSION_MISMATCHES
      = "stream_read_version_mismatches";

  /** {@value}. */
  public static final String STREAM_SEEK_BYTES_BACKWARDS
      = "stream_bytes_backwards_on_seek";

  /** {@value}. */
  public static final String STREAM_SEEK_BYTES_READ = "stream_seek_bytes_read";

  /** {@value}. */
  public static final String STREAM_SEEK_BYTES_SKIPPED
      = "stream_bytes_skipped_on_seek";

  /** {@value}. */
  public static final String STREAM_SEEK_OPERATIONS = "stream_seek_operations";

  /** {@value}. */
  public static final String STREAM_BACKWARD_SEEK_OPERATIONS = "stream_backward_seek_operations";

  /** {@value}. */
  public static final String STREAM_CLOSE_BYTES_READ
      = "stream_bytes_read_in_close";

  /** {@value}. */
  public static final String STREAM_ABORT_BYTES_DISCARDED
      = "stream_bytes_discarded_in_abort";

  /** {@value}. */
  public static final String STREAM_WRITE_FAILURES = "stream_write_failures";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS
      = "stream_write_block_uploads";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_ACTIVE
      = "stream_write_block_uploads_active";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_COMMITTED
      = "stream_write_block_uploads_committed";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_ABORTED
      = "stream_write_block_uploads_aborted";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_PENDING
      = "stream_write_block_uploads_pending";

  /** {@value}. */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING =
      "stream_write_block_uploads_data_pending";

  /** {@value}. */
  public static final String STREAM_WRITE_TOTAL_TIME
      = "stream_write_total_time";

  /** {@value}. */
  public static final String STREAM_WRITE_TOTAL_DATA
      = "stream_write_total_data";

  /** {@value}. */
  public static final String STREAM_WRITE_QUEUE_DURATION
      = "stream_write_queue_duration";

}
