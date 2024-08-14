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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.audit.AuditStatisticNames;
import org.apache.hadoop.fs.s3a.statistics.StatisticTypeEnum;
import org.apache.hadoop.fs.statistics.FileSystemStatisticNames;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static org.apache.hadoop.fs.s3a.statistics.StatisticTypeEnum.TYPE_COUNTER;
import static org.apache.hadoop.fs.s3a.statistics.StatisticTypeEnum.TYPE_DURATION;
import static org.apache.hadoop.fs.s3a.statistics.StatisticTypeEnum.TYPE_GAUGE;
import static org.apache.hadoop.fs.s3a.statistics.StatisticTypeEnum.TYPE_QUANTILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;

/**
 * Statistic which are collected in S3A.
 * Counter and duration statistics are published in
 * {@link S3AFileSystem#getStorageStatistics()}.
 * and as metrics in {@link S3AInstrumentation}.
 * <p>
 * Where possible, stream names come from {@link StreamStatisticNames}
 * and {@link StoreStatisticNames}
 * </p>
 */
@InterfaceStability.Unstable
public enum Statistic {

  /* Low-level duration counters */
  ACTION_EXECUTOR_ACQUIRED(
      StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED,
      "Executor acquired.",
      TYPE_DURATION),
  ACTION_HTTP_HEAD_REQUEST(
      StoreStatisticNames.ACTION_HTTP_HEAD_REQUEST,
      "HEAD request.",
      TYPE_DURATION),
  ACTION_FILE_OPENED(
      StoreStatisticNames.ACTION_FILE_OPENED,
      "File opened.",
      TYPE_DURATION),
  ACTION_HTTP_GET_REQUEST(
      StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
      "GET request.",
      TYPE_DURATION),

  /* FileSystem Level statistics */

  FILESYSTEM_INITIALIZATION(
      FileSystemStatisticNames.FILESYSTEM_INITIALIZATION,
      "Filesystem initialization",
      TYPE_DURATION),
  FILESYSTEM_CLOSE(
      FileSystemStatisticNames.FILESYSTEM_CLOSE,
      "Filesystem close",
      TYPE_DURATION),

  DIRECTORIES_CREATED("directories_created",
      "Total number of directories created through the object store.",
      TYPE_COUNTER),
  DIRECTORIES_DELETED("directories_deleted",
      "Total number of directories deleted through the object store.",
      TYPE_COUNTER),
  FILES_COPIED("files_copied",
      "Total number of files copied within the object store.",
      TYPE_COUNTER),
  FILES_COPIED_BYTES("files_copied_bytes",
      "Total number of bytes copied within the object store.",
      TYPE_COUNTER),
  FILES_CREATED("files_created",
      "Total number of files created through the object store.",
      TYPE_COUNTER),
  FILES_DELETED("files_deleted",
      "Total number of files deleted from the object store.",
      TYPE_COUNTER),
  FILES_DELETE_REJECTED("files_delete_rejected",
      "Total number of files whose delete request was rejected",
      TYPE_COUNTER),
  FAKE_DIRECTORIES_CREATED("fake_directories_created",
      "Total number of fake directory entries created in the object store.",
      TYPE_COUNTER),
  FAKE_DIRECTORIES_DELETED("fake_directories_deleted",
      "Total number of fake directory deletes submitted to object store.",
      TYPE_COUNTER),
  IGNORED_ERRORS("ignored_errors", "Errors caught and ignored",
      TYPE_COUNTER),

  INVOCATION_ABORT(
      StoreStatisticNames.OP_ABORT,
      "Calls of abort()",
      TYPE_DURATION),
  INVOCATION_ACCESS(
      StoreStatisticNames.OP_ACCESS,
      "Calls of access()",
      TYPE_DURATION),
  INVOCATION_BULK_DELETE(
      StoreStatisticNames.OP_BULK_DELETE,
      "Calls of bulk delete()",
      TYPE_COUNTER),
  INVOCATION_COPY_FROM_LOCAL_FILE(
      StoreStatisticNames.OP_COPY_FROM_LOCAL_FILE,
      "Calls of copyFromLocalFile()",
      TYPE_DURATION),
  INVOCATION_CREATE(
      StoreStatisticNames.OP_CREATE,
      "Calls of create()",
      TYPE_DURATION),
  INVOCATION_CREATE_FILE(
      StoreStatisticNames.OP_CREATE_FILE,
      "Calls of createFile()",
      TYPE_DURATION),
  INVOCATION_CREATE_NON_RECURSIVE(
      StoreStatisticNames.OP_CREATE_NON_RECURSIVE,
      "Calls of createNonRecursive()",
      TYPE_COUNTER),
  INVOCATION_DELETE(
      StoreStatisticNames.OP_DELETE,
      "Calls of delete()",
      TYPE_DURATION),
  INVOCATION_EXISTS(
      StoreStatisticNames.OP_EXISTS,
      "Calls of exists()",
      TYPE_DURATION),
  INVOCATION_GET_CONTENT_SUMMARY(
      StoreStatisticNames.OP_GET_CONTENT_SUMMARY,
      "Calls of getContentSummary()",
      TYPE_DURATION),
  INVOCATION_GET_DELEGATION_TOKEN(
      StoreStatisticNames.OP_GET_DELEGATION_TOKEN,
      "Calls of getDelegationToken()",
      TYPE_DURATION),
  INVOCATION_GET_FILE_CHECKSUM(
      StoreStatisticNames.OP_GET_FILE_CHECKSUM,
      "Calls of getFileChecksum()",
      TYPE_DURATION),
  INVOCATION_GET_FILE_STATUS(
      StoreStatisticNames.OP_GET_FILE_STATUS,
      "Calls of getFileStatus()",
      TYPE_DURATION),
  INVOCATION_GLOB_STATUS(
      StoreStatisticNames.OP_GLOB_STATUS,
      "Calls of globStatus()",
      TYPE_DURATION),
  INVOCATION_IS_DIRECTORY(
      StoreStatisticNames.OP_IS_DIRECTORY,
      "Calls of isDirectory()",
      TYPE_DURATION),
  INVOCATION_IS_FILE(
      StoreStatisticNames.OP_IS_FILE,
      "Calls of isFile()",
      TYPE_DURATION),
  INVOCATION_HFLUSH(
      StoreStatisticNames.OP_HFLUSH,
      "Calls of hflush()",
      TYPE_COUNTER),
  INVOCATION_HSYNC(
      StoreStatisticNames.OP_HSYNC,
      "Calls of hsync()",
      TYPE_COUNTER),
  INVOCATION_LIST_FILES(
      StoreStatisticNames.OP_LIST_FILES,
      "Calls of listFiles()",
      TYPE_DURATION),
  INVOCATION_LIST_LOCATED_STATUS(
      StoreStatisticNames.OP_LIST_LOCATED_STATUS,
      "Calls of listLocatedStatus()",
      TYPE_COUNTER),
  INVOCATION_LIST_STATUS(
      StoreStatisticNames.OP_LIST_STATUS,
      "Calls of listStatus()",
      TYPE_DURATION),
  INVOCATION_MKDIRS(
      StoreStatisticNames.OP_MKDIRS,
      "Calls of mkdirs()",
      TYPE_DURATION),
  INVOCATION_OPEN(
      StoreStatisticNames.OP_OPEN,
      "Calls of open()",
      TYPE_COUNTER),
  INVOCATION_OPENFILE(
      StoreStatisticNames.OP_OPENFILE,
      "Calls of openFile()",
      TYPE_COUNTER),
  INVOCATION_RENAME(
      StoreStatisticNames.OP_RENAME,
      "Calls of rename()",
      TYPE_DURATION),

  /* The XAttr API metrics are all durations */
  INVOCATION_XATTR_GET_MAP(
      StoreStatisticNames.OP_XATTR_GET_MAP,
      "Calls of getXAttrs(Path path)",
      TYPE_DURATION),
  INVOCATION_XATTR_GET_NAMED(
      StoreStatisticNames.OP_XATTR_GET_NAMED,
      "Calls of getXAttr(Path, String)",
      TYPE_DURATION),
  INVOCATION_XATTR_GET_NAMED_MAP(
      StoreStatisticNames.OP_XATTR_GET_NAMED_MAP,
      "Calls of xattr()",
      TYPE_DURATION),
  INVOCATION_OP_XATTR_LIST(
      StoreStatisticNames.OP_XATTR_LIST,
      "Calls of getXAttrs(Path path, List<String> names)",
      TYPE_DURATION),

  /* Object IO */
  OBJECT_COPY_REQUESTS(StoreStatisticNames.OBJECT_COPY_REQUESTS,
      "Object copy requests",
      TYPE_COUNTER),
  OBJECT_DELETE_REQUEST(StoreStatisticNames.OBJECT_DELETE_REQUEST,
      "Object delete requests",
      TYPE_DURATION),
  OBJECT_BULK_DELETE_REQUEST(StoreStatisticNames.OBJECT_BULK_DELETE_REQUEST,
      "Object bulk delete requests",
      TYPE_DURATION),
  OBJECT_DELETE_OBJECTS(StoreStatisticNames.OBJECT_DELETE_OBJECTS,
      "Objects deleted in delete requests",
      TYPE_COUNTER),
  OBJECT_LIST_REQUEST(StoreStatisticNames.OBJECT_LIST_REQUEST,
      "Count of object listings made",
      TYPE_DURATION),
  OBJECT_CONTINUE_LIST_REQUESTS(
      StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST,
      "Count of continued object listings made",
      TYPE_DURATION),
  OBJECT_METADATA_REQUESTS(
      StoreStatisticNames.OBJECT_METADATA_REQUESTS,
      "Count of requests for object metadata",
      TYPE_COUNTER),
  OBJECT_MULTIPART_UPLOAD_INITIATED(
      StoreStatisticNames.OBJECT_MULTIPART_UPLOAD_INITIATED,
      "Object multipart upload initiated",
      TYPE_DURATION),
  OBJECT_MULTIPART_UPLOAD_ABORTED(
      StoreStatisticNames.OBJECT_MULTIPART_UPLOAD_ABORTED,
      "Object multipart upload aborted",
      TYPE_DURATION),
  OBJECT_MULTIPART_UPLOAD_LIST(
      StoreStatisticNames.OBJECT_MULTIPART_UPLOAD_LIST,
      "Object multipart list request issued",
      TYPE_DURATION),  OBJECT_PUT_REQUESTS(
      StoreStatisticNames.OBJECT_PUT_REQUEST,
      "Object put/multipart upload count",
      TYPE_DURATION),
  OBJECT_PUT_REQUESTS_COMPLETED(
      StoreStatisticNames.OBJECT_PUT_REQUEST_COMPLETED,
      "Object put/multipart upload completed count",
      TYPE_COUNTER),
  OBJECT_PUT_REQUESTS_ACTIVE(
      StoreStatisticNames.OBJECT_PUT_REQUEST_ACTIVE,
      "Current number of active put requests",
      TYPE_GAUGE),
  OBJECT_PUT_BYTES(
      StoreStatisticNames.OBJECT_PUT_BYTES,
      "number of bytes uploaded",
      TYPE_COUNTER),
  OBJECT_PUT_BYTES_PENDING(
      StoreStatisticNames.OBJECT_PUT_BYTES_PENDING,
      "number of bytes queued for upload/being actively uploaded",
      TYPE_GAUGE),
  STREAM_READ_ABORTED(
      StreamStatisticNames.STREAM_READ_ABORTED,
      "Count of times the TCP stream was aborted",
      TYPE_COUNTER),

  /* Stream Reads */
  STREAM_READ_BYTES(
      StreamStatisticNames.STREAM_READ_BYTES,
      "Bytes read from an input stream in read() calls",
      TYPE_COUNTER),
  STREAM_READ_BYTES_DISCARDED_ABORT(
      StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT,
      "Count of bytes discarded by aborting an input stream",
      TYPE_COUNTER),
  STREAM_READ_BYTES_READ_CLOSE(
      StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE,
      "Count of bytes read and discarded when closing an input stream",
      TYPE_COUNTER),
  STREAM_READ_CLOSED(
      StreamStatisticNames.STREAM_READ_CLOSED,
      "Count of times the TCP stream was closed",
      TYPE_COUNTER),
  STREAM_READ_CLOSE_OPERATIONS(
      StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
      "Total count of times an attempt to close an input stream was made",
      TYPE_COUNTER),
  STREAM_READ_EXCEPTIONS(
      StreamStatisticNames.STREAM_READ_EXCEPTIONS,
      "Count of exceptions raised during input stream reads",
      TYPE_COUNTER),
  STREAM_READ_FULLY_OPERATIONS(
      StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS,
      "Count of readFully() operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_OPENED(
      StreamStatisticNames.STREAM_READ_OPENED,
      "Total count of times an input stream to object store data was opened",
      TYPE_COUNTER),
  STREAM_READ_OPERATIONS(
      StreamStatisticNames.STREAM_READ_OPERATIONS,
      "Count of read() operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_VECTORED_OPERATIONS(
          StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
          "Count of readVectored() operations in an input stream.",
          TYPE_COUNTER),
  STREAM_READ_VECTORED_READ_BYTES_DISCARDED(
          StreamStatisticNames.STREAM_READ_VECTORED_READ_BYTES_DISCARDED,
          "Count of bytes discarded during readVectored() operation." +
                  " in an input stream",
          TYPE_COUNTER),
  STREAM_READ_VECTORED_INCOMING_RANGES(
          StreamStatisticNames.STREAM_READ_VECTORED_INCOMING_RANGES,
          "Count of incoming file ranges during readVectored() operation.",
          TYPE_COUNTER),
  STREAM_READ_VECTORED_COMBINED_RANGES(
          StreamStatisticNames.STREAM_READ_VECTORED_COMBINED_RANGES,
          "Count of combined file ranges during readVectored() operation.",
          TYPE_COUNTER),
  STREAM_READ_REMOTE_STREAM_ABORTED(
      StreamStatisticNames.STREAM_READ_REMOTE_STREAM_ABORTED,
      "Duration of aborting a remote stream during stream IO",
      TYPE_DURATION),
  STREAM_READ_REMOTE_STREAM_CLOSED(
      StreamStatisticNames.STREAM_READ_REMOTE_STREAM_DRAINED,
      "Duration of closing a remote stream during stream IO",
      TYPE_DURATION),

  STREAM_READ_OPERATIONS_INCOMPLETE(
      StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
      "Count of incomplete read() operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_VERSION_MISMATCHES(
      StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES,
      "Count of version mismatches encountered while reading an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BACKWARD_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
      "Count of executed seek operations which went backwards in a stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_BACKWARDS(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
      "Count of bytes moved backwards during seek operations"
          + " in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_DISCARDED(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_DISCARDED,
      "Count of bytes read and discarded during seek() in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_SKIPPED(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
      "Count of bytes skipped during forward seek operations"
          + " an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_FORWARD_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
      "Count of executed seek operations which went forward in"
          + " an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
      "Count of seek operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_POLICY_CHANGED(
      StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED,
      "Count of times the seek policy was dynamically changed"
          + " in an input stream",
      TYPE_COUNTER),
  STREAM_READ_TOTAL_BYTES(
      StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
      "Total count of bytes read from an input stream",
      TYPE_COUNTER),
  STREAM_READ_UNBUFFERED(
      StreamStatisticNames.STREAM_READ_UNBUFFERED,
      "Total count of input stream unbuffering operations",
      TYPE_COUNTER),
  STREAM_READ_BLOCKS_IN_FILE_CACHE(
      StreamStatisticNames.STREAM_READ_BLOCKS_IN_FILE_CACHE,
      "Gauge of blocks in disk cache",
      TYPE_GAUGE),
  STREAM_READ_ACTIVE_PREFETCH_OPERATIONS(
      StreamStatisticNames.STREAM_READ_ACTIVE_PREFETCH_OPERATIONS,
      "Gauge of active prefetches",
      TYPE_GAUGE),
  STREAM_READ_ACTIVE_MEMORY_IN_USE(
      StreamStatisticNames.STREAM_READ_ACTIVE_MEMORY_IN_USE,
      "Gauge of active memory in use",
      TYPE_GAUGE),
  STREAM_READ_BLOCK_FETCH_OPERATIONS(
      StreamStatisticNames.STREAM_READ_BLOCK_FETCH_OPERATIONS,
      "Tracker/Gauge of active block fetches",
      TYPE_DURATION),
  STREAM_READ_BLOCK_ACQUIRE_AND_READ(
      StreamStatisticNames.STREAM_READ_BLOCK_ACQUIRE_AND_READ,
      "Time spent blocked waiting for blocks prefetch/read to complete",
      TYPE_DURATION),

  /* Stream read block prefetch/cache gauges */
  /* As with all gauges; aggregation is of limited value */
  STREAM_READ_BLOCK_CACHE_ENABLED(
      StreamStatisticNames.STREAM_READ_BLOCK_CACHE_ENABLED,
        "gauge to signal the stream caches blocks",
      TYPE_GAUGE),
  STREAM_READ_BLOCK_PREFETCH_ENABLED(
      StreamStatisticNames.STREAM_READ_BLOCK_PREFETCH_ENABLED,
        "gauge to signal the stream prefetches blocks",
      TYPE_GAUGE),
  STREAM_READ_BLOCK_PREFETCH_LIMIT(
      StreamStatisticNames.STREAM_READ_BLOCK_PREFETCH_LIMIT,
        "gauge to log prefetch limit",
      TYPE_GAUGE),
  STREAM_READ_BLOCK_SIZE(
      StreamStatisticNames.STREAM_READ_BLOCK_SIZE,
        "gauge to log stream block read size",
      TYPE_GAUGE),

  /* Stream Write statistics */

  STREAM_WRITE_EXCEPTIONS(
      StreamStatisticNames.STREAM_WRITE_EXCEPTIONS,
      "Count of stream write failures reported",
      TYPE_COUNTER),
  STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS(
      StreamStatisticNames.STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS,
      "Count of failures when finalizing a multipart upload",
      TYPE_COUNTER),
  STREAM_WRITE_BLOCK_UPLOADS(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS,
      "Count of block/partition uploads completed",
      TYPE_COUNTER),
  STREAM_WRITE_BLOCK_UPLOADS_ACTIVE(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_ACTIVE,
      "Count of block/partition uploads active",
      TYPE_GAUGE),
  STREAM_WRITE_BLOCK_UPLOADS_COMMITTED(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_COMMITTED,
      "Count of number of block uploads committed",
      TYPE_COUNTER),
  STREAM_WRITE_BLOCK_UPLOADS_ABORTED(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_ABORTED,
      "Count of number of block uploads aborted",
      TYPE_COUNTER),

  STREAM_WRITE_BLOCK_UPLOADS_PENDING(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_PENDING,
      "Gauge of block/partitions uploads queued to be written",
      TYPE_GAUGE),
  STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING(
      StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING,
      "Gauge of data queued to be written",
      TYPE_GAUGE),
  STREAM_WRITE_TOTAL_TIME(
      StreamStatisticNames.STREAM_WRITE_TOTAL_TIME,
      "Count of total time taken for uploads to complete",
      TYPE_COUNTER),
  STREAM_WRITE_TOTAL_DATA(StreamStatisticNames.STREAM_WRITE_TOTAL_DATA,
      "Count of total data uploaded",
      TYPE_COUNTER),
  STREAM_WRITE_BYTES(
      StreamStatisticNames.STREAM_WRITE_BYTES,
      "Count of bytes written to output stream"
          + " (including all not yet uploaded)",
      TYPE_COUNTER),
  STREAM_WRITE_QUEUE_DURATION(
      StreamStatisticNames.STREAM_WRITE_QUEUE_DURATION,
      "Total queue duration of all block uploads",
      TYPE_DURATION),

  /* Stream prefetch file cache eviction */
  STREAM_EVICT_BLOCKS_FROM_FILE_CACHE(
      StreamStatisticNames.STREAM_EVICT_BLOCKS_FROM_FILE_CACHE,
      "Count of blocks evicted from the disk cache",
      TYPE_COUNTER),
  STREAM_FILE_CACHE_EVICTION(
      StreamStatisticNames.STREAM_FILE_CACHE_EVICTION,
      "Duration of the eviction of an element from LRU cache that holds disk cache blocks",
      TYPE_DURATION),

  /* committer stats */
  COMMITTER_COMMITS_CREATED(
      "committer_commits_created",
      "Count of files to commit created",
      TYPE_COUNTER),
  COMMITTER_COMMITS_COMPLETED(
      "committer_commits_completed",
      "Count of files committed",
      TYPE_COUNTER),
  COMMITTER_COMMIT_JOB(
      "committer_commit_job",
      "Duration Tracking of time to commit an entire job",
      TYPE_DURATION),
  COMMITTER_JOBS_SUCCEEDED(
      "committer_jobs_completed",
      "Count of successful jobs",
      TYPE_COUNTER),
  COMMITTER_JOBS_FAILED(
      "committer_jobs_failed",
      "Count of failed jobs",
      TYPE_COUNTER),
  COMMITTER_TASKS_SUCCEEDED(
      "committer_tasks_completed",
      "Count of successful tasks",
      TYPE_COUNTER),
  COMMITTER_TASKS_FAILED(
      "committer_tasks_failed",
      "Count of failed tasks",
      TYPE_COUNTER),
  COMMITTER_BYTES_COMMITTED(
      "committer_bytes_committed",
      "Amount of data committed",
      TYPE_COUNTER),
  COMMITTER_BYTES_UPLOADED(
      "committer_bytes_uploaded",
      "Count of bytes uploaded duing commit operations",
      TYPE_COUNTER),
  COMMITTER_COMMITS_FAILED(
      "committer_commits"+ SUFFIX_FAILURES,
      "Count of commits failed",
      TYPE_COUNTER),
  COMMITTER_COMMITS_ABORTED(
      "committer_commits_aborted",
      "Count of commits aborted",
      TYPE_COUNTER),
  COMMITTER_COMMITS_REVERTED(
      "committer_commits_reverted",
      "Count of commits reverted",
      TYPE_COUNTER),
  COMMITTER_LOAD_SINGLE_PENDING_FILE(
      "committer_load_single_pending_file",
      "Duration to load a single pending file in task commit",
      TYPE_DURATION),
  COMMITTER_MAGIC_FILES_CREATED(
      "committer_magic_files_created",
      "Count of files created under 'magic' paths",
      TYPE_COUNTER),

  COMMITTER_MAGIC_MARKER_PUT(
      "committer_magic_marker_put",
      "Duration Tracking of marker files created under 'magic' paths",
      TYPE_DURATION),
  COMMITTER_MATERIALIZE_FILE(
      "committer_materialize_file",
      "Duration Tracking of time to materialize a file in job commit",
      TYPE_DURATION),
  COMMITTER_STAGE_FILE_UPLOAD(
      "committer_stage_file_upload",
      "Duration Tracking of files uploaded from a local staging path",
      TYPE_DURATION),

  /* General Store operations */
  STORE_CLIENT_CREATION(
      StoreStatisticNames.STORE_CLIENT_CREATION,
      "Store Client Creation",
      TYPE_DURATION),

  STORE_EXISTS_PROBE(StoreStatisticNames.STORE_EXISTS_PROBE,
      "Store Existence Probe",
      TYPE_DURATION),
  STORE_IO_REQUEST(StoreStatisticNames.STORE_IO_REQUEST,
      "requests made of the remote store",
      TYPE_COUNTER),

  STORE_IO_RETRY(StoreStatisticNames.STORE_IO_RETRY,
      "retried requests made of the remote store",
      TYPE_COUNTER),

  STORE_IO_RATE_LIMITED(StoreStatisticNames.STORE_IO_RATE_LIMITED_DURATION,
      "Duration of rate limited operations",
      TYPE_DURATION),

  STORE_IO_THROTTLED(
      StoreStatisticNames.STORE_IO_THROTTLED,
      "Requests throttled and retried",
      TYPE_COUNTER),
  STORE_IO_THROTTLE_RATE(
      StoreStatisticNames.STORE_IO_THROTTLE_RATE,
      "Rate of S3 request throttling",
      TYPE_QUANTILE),

  /*
   * Delegation Token Operations.
   */
  DELEGATION_TOKENS_ISSUED(
      StoreStatisticNames.DELEGATION_TOKENS_ISSUED,
      "Count of delegation tokens issued",
      TYPE_DURATION),

  /* Multipart Upload API */

  MULTIPART_UPLOAD_INSTANTIATED(
      StoreStatisticNames.MULTIPART_UPLOAD_INSTANTIATED,
      "Multipart Uploader Instantiated",
      TYPE_COUNTER),
  MULTIPART_UPLOAD_PART_PUT(
      StoreStatisticNames.MULTIPART_UPLOAD_PART_PUT,
      "Multipart Part Put Operation",
      TYPE_COUNTER),
  MULTIPART_UPLOAD_PART_PUT_BYTES(
      StoreStatisticNames.MULTIPART_UPLOAD_PART_PUT_BYTES,
      "Multipart Part Put Bytes",
      TYPE_COUNTER),
  MULTIPART_UPLOAD_ABORTED(
      StoreStatisticNames.MULTIPART_UPLOAD_ABORTED,
      "Multipart Upload Aborted",
      TYPE_COUNTER),
  MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED(
      StoreStatisticNames.MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED,
      "Multipart Upload Abort Under Path Invoked",
      TYPE_COUNTER),
  MULTIPART_UPLOAD_COMPLETED(
      StoreStatisticNames.MULTIPART_UPLOAD_COMPLETED,
      "Multipart Upload Completed",
      TYPE_COUNTER),
  MULTIPART_UPLOAD_LIST(
      StoreStatisticNames.MULTIPART_UPLOAD_LIST,
      "Multipart Upload List",
      TYPE_DURATION),
  MULTIPART_UPLOAD_STARTED(
      StoreStatisticNames.MULTIPART_UPLOAD_STARTED,
      "Multipart Upload Started",
      TYPE_COUNTER),

  /* Audit statistics. */
  /* If more are added: update AuditTestSupport to include the new values. */
  AUDIT_ACCESS_CHECK_FAILURE(
      AuditStatisticNames.AUDIT_ACCESS_CHECK_FAILURE,
      "Audit access check was rejected",
      TYPE_COUNTER),
  AUDIT_SPAN_CREATION(
      AuditStatisticNames.AUDIT_SPAN_CREATION,
      "Audit Span Created",
      TYPE_COUNTER),
  AUDIT_FAILURE(
      AuditStatisticNames.AUDIT_FAILURE,
      "Audit failure/rejection",
      TYPE_COUNTER),
  AUDIT_REQUEST_EXECUTION(
      AuditStatisticNames.AUDIT_REQUEST_EXECUTION,
      "AWS request made",
      TYPE_COUNTER),

  /* Client side encryption gauge */
  CLIENT_SIDE_ENCRYPTION_ENABLED(
      "client_side_encryption_enabled",
      "gauge to indicate if client side encryption is enabled",
      TYPE_GAUGE
  );


  /**
   * A map used to support the {@link #fromSymbol(String)} call.
   */
  private static final Map<String, Statistic> SYMBOL_MAP =
      new HashMap<>(Statistic.values().length);
  static {
    for (Statistic stat : values()) {
      SYMBOL_MAP.put(stat.getSymbol(), stat);
    }
  }


  /**
   * Statistic definition.
   * @param symbol name
   * @param description description.
   * @param type type
   */
  Statistic(String symbol, String description, StatisticTypeEnum type) {
    this.symbol = symbol;
    this.description = description;
    this.type = type;
  }

  /** Statistic name. */
  private final String symbol;

  /** Statistic description. */
  private final String description;

  /** Statistic type. */
  private final StatisticTypeEnum type;

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

  /**
   * What type is this statistic?
   * @return the type.
   */
  public StatisticTypeEnum getType() {
    return type;
  }
}
