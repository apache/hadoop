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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistic which are collected in S3A.
 * These statistics are available at a low level in {@link S3AStorageStatistics}
 * and as metrics in {@link S3AInstrumentation}.
 * <p></p>
 * Where possible, stream names come from {@link StreamStatisticNames}
 * and {@link StoreStatisticNames}
 *
 */
@InterfaceStability.Unstable
public enum Statistic {

  DIRECTORIES_CREATED("directories_created",
      "Total number of directories created through the object store."),
  DIRECTORIES_DELETED("directories_deleted",
      "Total number of directories deleted through the object store."),
  FILES_COPIED("files_copied",
      "Total number of files copied within the object store."),
  FILES_COPIED_BYTES("files_copied_bytes",
      "Total number of bytes copied within the object store."),
  FILES_CREATED("files_created",
      "Total number of files created through the object store."),
  FILES_DELETED("files_deleted",
      "Total number of files deleted from the object store."),
  FILES_DELETE_REJECTED("files_delete_rejected",
      "Total number of files whose delete request was rejected"),
  FAKE_DIRECTORIES_CREATED("fake_directories_created",
      "Total number of fake directory entries created in the object store."),
  FAKE_DIRECTORIES_DELETED("fake_directories_deleted",
      "Total number of fake directory deletes submitted to object store."),
  IGNORED_ERRORS("ignored_errors", "Errors caught and ignored"),
  INVOCATION_COPY_FROM_LOCAL_FILE(
      StoreStatisticNames.OP_COPY_FROM_LOCAL_FILE,
      "Calls of copyFromLocalFile()"),
  INVOCATION_CREATE(
      StoreStatisticNames.OP_CREATE,
      "Calls of create()"),
  INVOCATION_CREATE_NON_RECURSIVE(
      StoreStatisticNames.OP_CREATE_NON_RECURSIVE,
      "Calls of createNonRecursive()"),
  INVOCATION_DELETE(
      StoreStatisticNames.OP_DELETE,
      "Calls of delete()"),
  INVOCATION_EXISTS(
      StoreStatisticNames.OP_EXISTS,
      "Calls of exists()"),
  INVOCATION_GET_DELEGATION_TOKEN(
      StoreStatisticNames.OP_GET_DELEGATION_TOKEN,
      "Calls of getDelegationToken()"),
  INVOCATION_GET_FILE_CHECKSUM(
      StoreStatisticNames.OP_GET_FILE_CHECKSUM,
      "Calls of getFileChecksum()"),
  INVOCATION_GET_FILE_STATUS(
      StoreStatisticNames.OP_GET_FILE_STATUS,
      "Calls of getFileStatus()"),
  INVOCATION_GLOB_STATUS(
      StoreStatisticNames.OP_GLOB_STATUS,
      "Calls of globStatus()"),
  INVOCATION_IS_DIRECTORY(
      StoreStatisticNames.OP_IS_DIRECTORY,
      "Calls of isDirectory()"),
  INVOCATION_IS_FILE(
      StoreStatisticNames.OP_IS_FILE,
      "Calls of isFile()"),
  INVOCATION_LIST_FILES(
      StoreStatisticNames.OP_LIST_FILES,
      "Calls of listFiles()"),
  INVOCATION_LIST_LOCATED_STATUS(
      StoreStatisticNames.OP_LIST_LOCATED_STATUS,
      "Calls of listLocatedStatus()"),
  INVOCATION_LIST_STATUS(
      StoreStatisticNames.OP_LIST_STATUS,
      "Calls of listStatus()"),
  INVOCATION_MKDIRS(
      StoreStatisticNames.OP_MKDIRS,
      "Calls of mkdirs()"),
  INVOCATION_OPEN(
      StoreStatisticNames.OP_OPEN,
      "Calls of open()"),
  INVOCATION_RENAME(
      StoreStatisticNames.OP_RENAME,
      "Calls of rename()"),
  OBJECT_COPY_REQUESTS("object_copy_requests", "Object copy requests"),
  OBJECT_DELETE_REQUESTS("object_delete_requests", "Object delete requests"),
  OBJECT_DELETE_OBJECTS("object_delete_objects",
      "Objects deleted in delete requests"),
  OBJECT_LIST_REQUESTS(StoreStatisticNames.OBJECT_LIST_REQUEST,
      "Number of object listings made"),
  OBJECT_CONTINUE_LIST_REQUESTS(
      StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST,
      "Number of continued object listings made"),
  OBJECT_METADATA_REQUESTS(
      StoreStatisticNames.ACTION_HTTP_HEAD_REQUEST,
      "Number of requests for object metadata"),
  OBJECT_MULTIPART_UPLOAD_INITIATED(
      StoreStatisticNames.OBJECT_MULTIPART_UPLOAD_INITIATED,
      "Object multipart upload initiated"),
  OBJECT_MULTIPART_UPLOAD_ABORTED(
      StoreStatisticNames.OBJECT_MULTIPART_UPLOAD_ABORTED,
      "Object multipart upload aborted"),
  OBJECT_PUT_REQUESTS(
      StoreStatisticNames.OBJECT_PUT_REQUEST,
      "Object put/multipart upload count"),
  OBJECT_PUT_REQUESTS_COMPLETED(
      StoreStatisticNames.OBJECT_PUT_REQUEST_COMPLETED,
      "Object put/multipart upload completed count"),
  OBJECT_PUT_REQUESTS_ACTIVE(
      StoreStatisticNames.OBJECT_PUT_REQUEST_ACTIVE,
      "Current number of active put requests"),
  OBJECT_PUT_BYTES(
      StoreStatisticNames.OBJECT_PUT_BYTES,
      "number of bytes uploaded"),
  OBJECT_PUT_BYTES_PENDING(
      StoreStatisticNames.OBJECT_PUT_BYTES_PENDING,
      "number of bytes queued for upload/being actively uploaded"),
  OBJECT_SELECT_REQUESTS(
      StoreStatisticNames.OBJECT_SELECT_REQUESTS,
      "Count of S3 Select requests issued"),
  STREAM_READ_ABORTED(
      StreamStatisticNames.STREAM_READ_ABORTED,
      "Count of times the TCP stream was aborted"),
  STREAM_READ_BYTES(
      StreamStatisticNames.STREAM_READ_BYTES,
      "Bytes read from an input stream in read() calls"),
  STREAM_READ_CLOSED(
      StreamStatisticNames.STREAM_READ_CLOSED,
      "Count of times the TCP stream was closed"),
  STREAM_READ_CLOSE_OPERATIONS(
      StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
      "Total count of times an attempt to close an input stream was made"),
  STREAM_READ_EXCEPTIONS(
      StreamStatisticNames.STREAM_READ_EXCEPTIONS,
      "Number of exceptions raised during input stream reads"),
  STREAM_READ_FULLY_OPERATIONS(
      StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS,
      "Count of readFully() operations in an input stream"),
  STREAM_READ_OPENED(
      StreamStatisticNames.STREAM_READ_OPENED,
      "Total count of times an input stream to object store data was opened"),
  STREAM_READ_OPERATIONS(
      StreamStatisticNames.STREAM_READ_OPERATIONS,
      "Count of read() operations in an input stream"),
  STREAM_READ_OPERATIONS_INCOMPLETE(
      StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
      "Count of incomplete read() operations in an input stream"),
  STREAM_READ_VERSION_MISMATCHES(
      StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES,
      "Count of version mismatches encountered while reading an input stream"),
  STREAM_READ_SEEK_BACKWARD_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
      "Number of executed seek operations which went backwards in a stream"),
  STREAM_READ_SEEK_BYTES_BACKWARDS(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
      "Count of bytes moved backwards during seek operations"
          + " in an input stream"),
  STREAM_READ_SEEK_BYTES_READ(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_READ,
      "Count of bytes read during seek() in an input stream"),
  STREAM_READ_SEEK_BYTES_SKIPPED(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
      "Count of bytes skipped during forward seek operations"
          + " an input stream"),
  STREAM_READ_SEEK_FORWARD_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
      "Number of executed seek operations which went forward in"
          + " an input stream"),
  STREAM_READ_SEEK_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
      "Number of seek operations in an input stream"),
  STREAM_READ_SEEK_POLICY_CHANGED(
      StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED,
      "Count of times the seek policy was dynamically changed"
          + " in an input stream"),
  STREAM_READ_CLOSE_BYTES_READ(
      StreamStatisticNames.STREAM_READ_CLOSE_BYTES_READ,
      "Count of bytes read when closing an input stream"),
  STREAM_READ_BYTES_DISCARDED_ABORT(
      StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT,
      "Count of bytes discarded by aborting in an input stream"),
  STREAM_READ_TOTAL_BYTES(
      StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
      "Total count of read from an input stream calls"),

  STREAM_WRITE_FAILURES(
      StreamStatisticNames.STREAM_WRITE_EXCEPTIONS,
      "Count of stream write failures reported"),
  STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS(
      StreamStatisticNames.STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS,
      "failures when finalizing a multipart upload"),
  STREAM_WRITE_BLOCK_UPLOADS(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS,
      "Count of block/partition uploads completed"),
  STREAM_WRITE_BLOCK_UPLOADS_ACTIVE(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_ACTIVE,
      "Count of block/partition uploads completed"),
  STREAM_WRITE_BLOCK_UPLOADS_COMMITTED(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_COMMITTED,
      "Count of number of block uploads committed"),
  STREAM_WRITE_BLOCK_UPLOADS_ABORTED(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_ABORTED,
      "Count of number of block uploads aborted"),

  STREAM_WRITE_BLOCK_UPLOADS_PENDING(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_PENDING,
      "Gauge of block/partitions uploads queued to be written"),
  STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING,
      "Gauge of block/partitions data uploads queued to be written"),
  STREAM_WRITE_TOTAL_TIME(
      StreamStatisticNames.STREAM_WRITE_TOTAL_TIME,
      "Count of total time taken for uploads to complete"),
  STREAM_WRITE_TOTAL_DATA("stream_write_total_data",
      "Count of total data uploaded"),
  STREAM_WRITE_BYTES(
      StreamStatisticNames.STREAM_WRITE_BYTES,
      "Count of bytes written to output stream"
          + " (including all not yet uploaded"),
  STREAM_WRITE_QUEUE_DURATION(
      StreamStatisticNames.STREAM_WRITE_QUEUE_DURATION,
      "Total queue duration of all block uploads"),

  // S3guard committer stats
  COMMITTER_COMMITS_CREATED(
      "committer_commits_created",
      "Number of files to commit created"),
  COMMITTER_COMMITS_COMPLETED(
      "committer_commits_completed",
      "Number of files committed"),
  COMMITTER_JOBS_SUCCEEDED(
      "committer_jobs_completed",
      "Number of successful jobs"),
  COMMITTER_JOBS_FAILED(
      "committer_jobs_failed",
      "Number of failed jobs"),
  COMMITTER_TASKS_SUCCEEDED(
      "committer_tasks_completed",
      "Number of successful tasks"),
  COMMITTER_TASKS_FAILED(
      "committer_tasks_failed",
      "Number of failed tasks"),
  COMMITTER_BYTES_COMMITTED(
      "committer_bytes_committed",
      "Amount of data committed"),
  COMMITTER_BYTES_UPLOADED(
      "committer_bytes_uploaded",
      "Number of bytes uploaded duing commit operations"),
  COMMITTER_COMMITS_FAILED(
      "committer_commits_failed",
      "Number of commits failed"),
  COMMITTER_COMMITS_ABORTED(
      "committer_commits_aborted",
      "Number of commits aborted"),
  COMMITTER_COMMITS_REVERTED(
      "committer_commits_reverted",
      "Number of commits reverted"),
  COMMITTER_MAGIC_FILES_CREATED(
      "committer_magic_files_created",
      "Number of files created under 'magic' paths"),

  // S3guard stats
  S3GUARD_METADATASTORE_PUT_PATH_REQUEST(
      "s3guard_metadatastore_put_path_request",
      "S3Guard metadata store put one metadata path request"),
  S3GUARD_METADATASTORE_PUT_PATH_LATENCY(
      "s3guard_metadatastore_put_path_latency",
      "S3Guard metadata store put one metadata path latency"),
  S3GUARD_METADATASTORE_INITIALIZATION("s3guard_metadatastore_initialization",
      "S3Guard metadata store initialization times"),
  S3GUARD_METADATASTORE_RECORD_DELETES(
      "s3guard_metadatastore_record_deletes",
      "S3Guard metadata store records deleted"),
  S3GUARD_METADATASTORE_RECORD_READS(
      "s3guard_metadatastore_record_reads",
      "S3Guard metadata store records read"),
  S3GUARD_METADATASTORE_RECORD_WRITES(
      "s3guard_metadatastore_record_writes",
      "S3Guard metadata store records written"),
  S3GUARD_METADATASTORE_RETRY("s3guard_metadatastore_retry",
      "S3Guard metadata store retry events"),
  S3GUARD_METADATASTORE_THROTTLED("s3guard_metadatastore_throttled",
      "S3Guard metadata store throttled events"),
  S3GUARD_METADATASTORE_THROTTLE_RATE(
      "s3guard_metadatastore_throttle_rate",
      "S3Guard metadata store throttle rate"),
  S3GUARD_METADATASTORE_AUTHORITATIVE_DIRECTORIES_UPDATED(
      "s3guard_metadatastore_authoritative_directories_updated",
      "S3Guard metadata store authoritative directories updated from S3"),

  STORE_IO_THROTTLED("store_io_throttled", "Requests throttled and retried"),
  STORE_IO_THROTTLE_RATE("store_io_throttle_rate",
      "Rate of S3 request throttling"),

  DELEGATION_TOKENS_ISSUED("delegation_tokens_issued",
      "Number of delegation tokens issued"),

  MULTIPART_INSTANTIATED(
      "multipart_instantiated",
      "Multipart Uploader Instantiated"),
  MULTIPART_PART_PUT(
      "multipart_part_put",
      "Multipart Part Put Operation"),
  MULTIPART_PART_PUT_BYTES(
      "multipart_part_put_bytes",
      "Multipart Part Put Bytes"),
  MULTIPART_UPLOAD_ABORTED(
      "multipart_upload_aborted",
      "Multipart Upload Aborted"),
  MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED(
      "multipart_upload_abort_under_path_invoked",
      "Multipart Upload Abort Udner Path Invoked"),
  MULTIPART_UPLOAD_COMPLETED(
      "multipart_upload_completed",
      "Multipart Upload Completed"),
  MULTIPART_UPLOAD_STARTED(
      "multipart_upload_started",
      "Multipart Upload Started"),

  STORE_IO_REQUEST(StoreStatisticNames.STORE_IO_REQUEST,
      "requests made of the remote store"),

  STORE_IO_RETRY(StoreStatisticNames.STORE_IO_RETRY,
      "retried requests made of the remote store");

  private static final Map<String, Statistic> SYMBOL_MAP =
      new HashMap<>(Statistic.values().length);
  static {
    for (Statistic stat : values()) {
      SYMBOL_MAP.put(stat.getSymbol(), stat);
    }
  }

  Statistic(String symbol, String description) {
    this.symbol = symbol;
    this.description = description;
  }

  private final String symbol;
  private final String description;

  public String getSymbol() {
    return symbol;
  }

  /**
   * Get a statistic from a symbol.
   * @param symbol statistic to look up
   * @return the value or null.
   */
  public static Statistic fromSymbol(String symbol) {
    return SYMBOL_MAP.get(symbol);
  }

  public String getDescription() {
    return description;
  }

  /**
   * The string value is simply the symbol.
   * This makes this operation very low cost.
   * @return the symbol of this statistic.
   */
  @Override
  public String toString() {
    return symbol;
  }
}
