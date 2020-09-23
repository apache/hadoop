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
 *   <li>the name of the constants are uppercase, words separated by
 *   underscores.</li>
 *   <li>the value of the constants are lowercase of the constant names.</li>
 * </ul>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class StoreStatisticNames {

  /** {@value}. */
  public static final String OP_APPEND = "op_append";

  /** {@value}. */
  public static final String OP_COPY_FROM_LOCAL_FILE =
      "op_copy_from_local_file";

  /** {@value}. */
  public static final String OP_CREATE = "op_create";

  /** {@value}. */
  public static final String OP_CREATE_NON_RECURSIVE =
      "op_create_non_recursive";

  /** {@value}. */
  public static final String OP_DELETE = "op_delete";

  /** {@value}. */
  public static final String OP_EXISTS = "op_exists";

  /** {@value}. */
  public static final String OP_GET_CONTENT_SUMMARY =
      "op_get_content_summary";

  /** {@value}. */
  public static final String OP_GET_DELEGATION_TOKEN =
      "op_get_delegation_token";

  /** {@value}. */
  public static final String OP_GET_FILE_CHECKSUM =
      "op_get_file_checksum";

  /** {@value}. */
  public static final String OP_GET_FILE_STATUS = "op_get_file_status";

  /** {@value}. */
  public static final String OP_GET_STATUS = "op_get_status";

  /** {@value}. */
  public static final String OP_GLOB_STATUS = "op_glob_status";

  /** {@value}. */
  public static final String OP_IS_FILE = "op_is_file";

  /** {@value}. */
  public static final String OP_IS_DIRECTORY = "op_is_directory";

  /** {@value}. */
  public static final String OP_LIST_FILES = "op_list_files";

  /** {@value}. */
  public static final String OP_LIST_LOCATED_STATUS =
      "op_list_located_status";

  /** {@value}. */
  public static final String OP_LIST_STATUS = "op_list_status";

  /** {@value}. */
  public static final String OP_MKDIRS = "op_mkdirs";

  /** {@value}. */
  public static final String OP_MODIFY_ACL_ENTRIES = "op_modify_acl_entries";

  /** {@value}. */
  public static final String OP_OPEN = "op_open";

  /** {@value}. */
  public static final String OP_REMOVE_ACL = "op_remove_acl";

  /** {@value}. */
  public static final String OP_REMOVE_ACL_ENTRIES = "op_remove_acl_entries";

  /** {@value}. */
  public static final String OP_REMOVE_DEFAULT_ACL = "op_remove_default_acl";

  /** {@value}. */
  public static final String OP_RENAME = "op_rename";

  /** {@value}. */
  public static final String OP_SET_ACL = "op_set_acl";

  /** {@value}. */
  public static final String OP_SET_OWNER = "op_set_owner";

  /** {@value}. */
  public static final String OP_SET_PERMISSION = "op_set_permission";

  /** {@value}. */
  public static final String OP_SET_TIMES = "op_set_times";

  /** {@value}. */
  public static final String OP_TRUNCATE = "op_truncate";

  /** {@value}. */
  public static final String DELEGATION_TOKENS_ISSUED
      = "delegation_tokens_issued";

  /** Requests throttled and retried: {@value}. */
  public static final String STORE_IO_THROTTLED
      = "store_io_throttled";

  /** Requests made of a store: {@value}. */
  public static final String STORE_IO_REQUEST
      = "store_io_request";

  /**
   * IO retried: {@value}.
   */
  public static final String STORE_IO_RETRY
      = "store_io_retry";

  /**
   * A store's equivalent of a paged LIST request was initiated: {@value}.
   */
  public static final String OBJECT_LIST_REQUEST
      = "object_list_request";

  /**
   * Number of continued object listings made.
   * Value :{@value}.
   */
  public static final String OBJECT_CONTINUE_LIST_REQUEST =
      "object_continue_list_request";

  /**
   * A store's equivalent of a DELETE request was made: {@value}.
   * This may be an HTTP DELETE verb, or it may be some custom
   * operation which takes a list of objects to delete.
   */
  public static final String OP_HTTP_DELETE_REQUEST
      = "op_http_delete_request";

  /**
   * Object multipart upload initiated.
   * Value :{@value}.
   */
  public static final String OBJECT_MULTIPART_UPLOAD_INITIATED =
      "object_multipart_initiated";

  /**
   * Object multipart upload aborted.
   * Value :{@value}.
   */
  public static final String OBJECT_MULTIPART_UPLOAD_ABORTED =
      "object_multipart_aborted";

  /**
   * Object put/multipart upload count.
   * Value :{@value}.
   */
  public static final String OBJECT_PUT_REQUEST =
      "object_put_request";

  /**
   * Object put/multipart upload completed count.
   * Value :{@value}.
   */
  public static final String OBJECT_PUT_REQUEST_COMPLETED =
      "object_put_request_completed";

  /**
   * Current number of active put requests.
   * Value :{@value}.
   */
  public static final String OBJECT_PUT_REQUEST_ACTIVE =
      "object_put_request_active";

  /**
   * number of bytes uploaded.
   * Value :{@value}.
   */
  public static final String OBJECT_PUT_BYTES =
      "object_put_bytes";

  /**
   * number of bytes queued for upload/being actively uploaded.
   * Value :{@value}.
   */
  public static final String OBJECT_PUT_BYTES_PENDING =
      "object_put_bytes_pending";

  /**
   * Count of S3 Select (or similar) requests issued.
   * Value :{@value}.
   */
  public static final String OBJECT_SELECT_REQUESTS =
      "object_select_requests";

  /**
   * Suffix to use for a minimum value when
   * the same key is shared across min/mean/max
   * statistics.
   * <p></p>
   * Value {@value}.
   */
  public static final String SUFFIX_MIN = ".min";

  /**
   * Suffix to use for a maximum value when
   * the same key is shared across max/mean/max
   * statistics.
   * <p></p>
   * Value {@value}.
   */
  public static final String SUFFIX_MAX = ".max";

  /**
   * Suffix to use for a mean statistic value when
   * the same key is shared across mean/mean/max
   * statistics.
   * <p></p>
   * Value {@value}.
   */
  public static final String SUFFIX_MEAN = ".mean";


  /**
   * The name of the statistic collected for executor acquisition if
   * a duration tracker factory is passed in to the constructor.
   * {@value}.
   */
  public static final String ACTION_EXECUTOR_ACQUIRED =
      "action_executor_acquired";

  /**
   * An HTTP HEAD request was made: {@value}.
   */
  public static final String ACTION_HTTP_HEAD_REQUEST
      = "action_http_head_request";

  /**
   * An HTTP GET request was made: {@value}.
   */
  public static final String ACTION_HTTP_GET_REQUEST
      = "action_http_get_request";


  private StoreStatisticNames() {
  }

}
