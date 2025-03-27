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

package org.apache.hadoop.fs.s3a.impl;

/**
 * Enum for progress listener events.
 * Some are used in the {@code S3ABlockOutputStream}
 * class to manage progress; others are to assist
 * testing.
 */
public enum ProgressListenerEvent {

  /**
   * Stream has been closed.
   */
  CLOSE_EVENT,

  /** PUT operation completed successfully. */
  PUT_COMPLETED_EVENT,

  /** PUT operation was interrupted. */
  PUT_INTERRUPTED_EVENT,

  /** PUT operation was interrupted. */
  PUT_FAILED_EVENT,

  /** A PUT operation was started. */
  PUT_STARTED_EVENT,

  /** Bytes were transferred. */
  REQUEST_BYTE_TRANSFER_EVENT,

  /**
   * A multipart upload was initiated.
   */
  TRANSFER_MULTIPART_INITIATED_EVENT,

  /**
   * A multipart upload was aborted.
   */
  TRANSFER_MULTIPART_ABORTED_EVENT,

  /**
   * A multipart upload was successfully.
   */
  TRANSFER_MULTIPART_COMPLETED_EVENT,

  /**
   * An upload of a part of a multipart upload was started.
   */
  TRANSFER_PART_STARTED_EVENT,

  /**
   * An upload of a part of a multipart upload was completed.
   * This does not indicate the upload was successful.
   */
  TRANSFER_PART_COMPLETED_EVENT,

  /**
   * An upload of a part of a multipart upload was completed
   * successfully.
   */
  TRANSFER_PART_SUCCESS_EVENT,

  /**
   * An upload of a part of a multipart upload was abported.
   */
  TRANSFER_PART_ABORTED_EVENT,

  /**
   * An upload of a part of a multipart upload failed.
   */
  TRANSFER_PART_FAILED_EVENT,

}
