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

  /**
   * Count of times the TCP stream was aborted.
   * Value: {@value}.
   */
  public static final String STREAM_READ_ABORTED = "stream_aborted";

  /**
   * Bytes read from an input stream in read() calls.
   * Does not include bytes read and then discarded in seek/close etc.
   * These are the bytes returned to the caller.
   * Value: {@value}.
   */
  public static final String STREAM_READ_BYTES
      = "stream_read_bytes";

  /**
   * Count of bytes discarded by aborting an input stream .
   * Value: {@value}.
   */
  public static final String STREAM_READ_BYTES_DISCARDED_ABORT
      = "stream_read_bytes_discarded_in_abort";

  /**
   * Count of bytes read and discarded when closing an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_BYTES_DISCARDED_CLOSE
      = "stream_read_bytes_discarded_in_close";

  /**
   * Count of times the TCP stream was closed.
   * Value: {@value}.
   */
  public static final String STREAM_READ_CLOSED = "stream_read_closed";

  /**
   * Total count of times an attempt to close an input stream was made
   * Value: {@value}.
   */
  public static final String STREAM_READ_CLOSE_OPERATIONS
      = "stream_read_close_operations";

  /**
   * Total count of times an input stream to was opened.
   * For object stores, that means the count a GET request was initiated.
   * Value: {@value}.
   */
  public static final String STREAM_READ_OPENED = "stream_read_opened";

  /**
   * Count of exceptions raised during input stream reads.
   * Value: {@value}.
   */
  public static final String STREAM_READ_EXCEPTIONS =
      "stream_read_exceptions";

  /**
   * Count of readFully() operations in an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_FULLY_OPERATIONS
      = "stream_read_fully_operations";

  /**
   * Count of read() operations in an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_OPERATIONS =
      "stream_read_operations";

  /**
   * Count of incomplete read() operations in an input stream,
   * that is, when the bytes returned were less than that requested.
   * Value: {@value}.
   */
  public static final String STREAM_READ_OPERATIONS_INCOMPLETE
      = "stream_read_operations_incomplete";

  /**
   * Count of version mismatches encountered while reading an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_VERSION_MISMATCHES
      = "stream_read_version_mismatches";

  /**
   * Count of executed seek operations which went backwards in a stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SEEK_BACKWARD_OPERATIONS =
      "stream_read_seek_backward_operations";

  /**
   * Count of bytes moved backwards during seek operations
   * in an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SEEK_BYTES_BACKWARDS
      = "stream_read_bytes_backwards_on_seek";

  /**
   * Count of bytes read and discarded during seek() in an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SEEK_BYTES_DISCARDED =
      "stream_read_seek_bytes_discarded";

  /**
   * Count of bytes skipped during forward seek operations.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SEEK_BYTES_SKIPPED
      = "stream_read_seek_bytes_skipped";

  /**
   * Count of executed seek operations which went forward in
   * an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SEEK_FORWARD_OPERATIONS
      = "stream_read_seek_forward_operations";

  /**
   * Count of times the seek policy was dynamically changed
   * in an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SEEK_POLICY_CHANGED =
      "stream_read_seek_policy_changed";

  /**
   * Count of seek operations in an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SEEK_OPERATIONS =
      "stream_read_seek_operations";

  /**
   * Count of {@code InputStream.skip()} calls.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SKIP_OPERATIONS =
      "stream_read_skip_operations";

  /**
   * Count bytes skipped in {@code InputStream.skip()} calls.
   * Value: {@value}.
   */
  public static final String STREAM_READ_SKIP_BYTES =
      "stream_read_skip_bytes";

  /**
   * Total count of bytes read from an input stream.
   * Value: {@value}.
   */
  public static final String STREAM_READ_TOTAL_BYTES
      = "stream_read_total_bytes";

  /**
   * Count of calls of {@code CanUnbuffer.unbuffer()}.
   * Value: {@value}.
   */
  public static final String STREAM_READ_UNBUFFERED
      = "stream_read_unbuffered";

  /**
   * "Count of stream write failures reported.
   * Value: {@value}.
   */
  public static final String STREAM_WRITE_EXCEPTIONS =
      "stream_write_exceptions";

  /**
   * Count of failures when finalizing a multipart upload:
   * {@value}.
   */
  public static final String STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS =
      "stream_write_exceptions_completing_upload";

  /**
   * Count of block/partition uploads complete.
   * Value: {@value}.
   */
  public static final String STREAM_WRITE_BLOCK_UPLOADS
      = "stream_write_block_uploads";

  /**
   * Count of number of block uploads aborted.
   * Value: {@value}.
   */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_ABORTED
      = "stream_write_block_uploads_aborted";

  /**
   * Count of block/partition uploads active.
   * Value: {@value}.
   */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_ACTIVE
      = "stream_write_block_uploads_active";

  /**
   * Gauge of data queued to be written.
   * Value: {@value}.
   */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING =
      "stream_write_block_uploads_data_pending";

  /**
   * Count of number of block uploads committed.
   * Value: {@value}.
   */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_COMMITTED
      = "stream_write_block_uploads_committed";

  /**
   * Gauge of block/partitions uploads queued to be written.
   * Value: {@value}.
   */
  public static final String STREAM_WRITE_BLOCK_UPLOADS_PENDING
      = "stream_write_block_uploads_pending";


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

  public static final String STREAM_WRITE_TOTAL_DATA
      = "stream_write_total_data";

  /**
   * Number of bytes to upload from an OutputStream.
   */
  public static final String BYTES_TO_UPLOAD
      = "bytes_upload";

  /**
   * Number of bytes uploaded successfully to the object store.
   */
  public static final String BYTES_UPLOAD_SUCCESSFUL
      = "bytes_upload_successfully";

  /**
   * Number of bytes failed to upload to the object store.
   */
  public static final String BYTES_UPLOAD_FAILED
      = "bytes_upload_failed";

  /**
   * Total time spent on waiting for a task to complete.
   */
  public static final String TIME_SPENT_ON_TASK_WAIT
      = "time_spent_task_wait";

  /**
   * Number of task queue shrunk operations.
   */
  public static final String QUEUE_SHRUNK_OPS
      = "queue_shrunk_ops";

  /**
   * Number of times current buffer is written to the service.
   */
  public static final String WRITE_CURRENT_BUFFER_OPERATIONS
      = "write_current_buffer_ops";

  /**
   * Total time spent on completing a PUT request.
   */
  public static final String TIME_SPENT_ON_PUT_REQUEST
      = "time_spent_on_put_request";

  /**
   * Number of seeks in buffer.
   */
  public static final String SEEK_IN_BUFFER
      = "seek_in_buffer";

  /**
   * Number of bytes read from the buffer.
   */
  public static final String BYTES_READ_BUFFER
      = "bytes_read_buffer";

  /**
   * Total number of remote read operations performed.
   */
  public static final String REMOTE_READ_OP
      = "remote_read_op";

  /**
   * Total number of bytes read from readAhead.
   */
  public static final String READ_AHEAD_BYTES_READ
      = "read_ahead_bytes_read";

  /**
   * Total number of bytes read from remote operations.
   */
  public static final String REMOTE_BYTES_READ
      = "remote_bytes_read";

  private StreamStatisticNames() {
  }

}
