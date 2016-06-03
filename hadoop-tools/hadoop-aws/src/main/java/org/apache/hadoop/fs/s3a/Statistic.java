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

/**
 * Statistic which are collected in S3A.
 * These statistics are available at a low level in {@link S3AStorageStatistics}
 * and as metrics in {@link S3AInstrumentation}
 */
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
  IGNORED_ERRORS("ignored_errors", "Errors caught and ignored"),
  INVOCATION_COPY_FROM_LOCAL_FILE("invocations_copyfromlocalfile",
      "Calls of copyFromLocalFile()"),
  INVOCATION_EXISTS("invocations_exists",
      "Calls of exists()"),
  INVOCATION_GET_FILE_STATUS("invocations_getfilestatus",
      "Calls of getFileStatus()"),
  INVOCATION_GLOB_STATUS("invocations_globstatus",
      "Calls of globStatus()"),
  INVOCATION_IS_DIRECTORY("invocations_is_directory",
      "Calls of isDirectory()"),
  INVOCATION_IS_FILE("invocations_is_file",
      "Calls of isFile()"),
  INVOCATION_LIST_FILES("invocations_listfiles",
      "Calls of listFiles()"),
  INVOCATION_LIST_LOCATED_STATUS("invocations_listlocatedstatus",
      "Calls of listLocatedStatus()"),
  INVOCATION_LIST_STATUS("invocations_liststatus",
      "Calls of listStatus()"),
  INVOCATION_MKDIRS("invocations_mdkirs",
      "Calls of mkdirs()"),
  INVOCATION_RENAME("invocations_rename",
      "Calls of rename()"),
  OBJECT_COPY_REQUESTS("object_copy_requests", "Object copy requests"),
  OBJECT_DELETE_REQUESTS("object_delete_requests", "Object delete requests"),
  OBJECT_LIST_REQUESTS("object_list_requests",
      "Number of object listings made"),
  OBJECT_METADATA_REQUESTS("object_metadata_requests",
      "Number of requests for object metadata"),
  OBJECT_MULTIPART_UPLOAD_ABORTED("object_multipart_aborted",
      "Object multipart upload aborted"),
  OBJECT_PUT_REQUESTS("object_put_requests",
      "Object put/multipart upload count"),
  OBJECT_PUT_BYTES("object_put_bytes", "number of bytes uploaded"),
  STREAM_ABORTED("streamAborted",
      "Count of times the TCP stream was aborted"),
  STREAM_BACKWARD_SEEK_OPERATIONS("streamBackwardSeekOperations",
      "Number of executed seek operations which went backwards in a stream"),
  STREAM_CLOSED("streamClosed", "Count of times the TCP stream was closed"),
  STREAM_CLOSE_OPERATIONS("streamCloseOperations",
      "Total count of times an attempt to close a data stream was made"),
  STREAM_FORWARD_SEEK_OPERATIONS("streamForwardSeekOperations",
      "Number of executed seek operations which went forward in a stream"),
  STREAM_OPENED("streamOpened",
      "Total count of times an input stream to object store was opened"),
  STREAM_READ_EXCEPTIONS("streamReadExceptions",
      "Number of seek operations invoked on input streams"),
  STREAM_READ_FULLY_OPERATIONS("streamReadFullyOperations",
      "count of readFully() operations in streams"),
  STREAM_READ_OPERATIONS("streamReadOperations",
      "Count of read() operations in streams"),
  STREAM_READ_OPERATIONS_INCOMPLETE("streamReadOperationsIncomplete",
      "Count of incomplete read() operations in streams"),
  STREAM_SEEK_BYTES_BACKWARDS("streamBytesBackwardsOnSeek",
      "Count of bytes moved backwards during seek operations"),
  STREAM_SEEK_BYTES_READ("streamBytesRead",
      "Count of bytes read during seek() in stream operations"),
  STREAM_SEEK_BYTES_SKIPPED("streamBytesSkippedOnSeek",
      "Count of bytes skipped during forward seek operation"),
  STREAM_SEEK_OPERATIONS("streamSeekOperations",
      "Number of read exceptions caught and attempted to recovered from");

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
    if (symbol != null) {
      for (Statistic opType : values()) {
        if (opType.getSymbol().equals(symbol)) {
          return opType;
        }
      }
    }
    return null;
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
