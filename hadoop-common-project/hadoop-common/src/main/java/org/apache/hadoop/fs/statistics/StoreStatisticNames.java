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
 * Common statistic names for object store operations..
 * <p>
 * When adding new common statistic name constants, please make them unique.
 * By convention:
 * </p>
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
  public static final String OP_ABORT = "op_abort";

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
  public static final String OP_HFLUSH = "op_hflush";

  /** {@value}. */
  public static final String OP_HSYNC = "op_hsync";

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

  /* The XAttr API */

  /** Invoke {@code getXAttrs(Path path)}: {@value}. */
  public static final String OP_XATTR_GET_MAP = "op_xattr_get_map";

  /** Invoke {@code getXAttr(Path, String)}: {@value}. */
  public static final String OP_XATTR_GET_NAMED = "op_xattr_get_named";

  /**
   * Invoke {@code getXAttrs(Path path, List<String> names)}: {@value}.
   */
  public static final String OP_XATTR_GET_NAMED_MAP =
      "op_xattr_get_named_map";

  /** Invoke {@code listXAttrs(Path path)}: {@value}. */
  public static final String OP_XATTR_LIST = "op_xattr_list";


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
   * A bulk DELETE request was made: {@value}.
   * A separate statistic from {@link #OBJECT_DELETE_REQUEST}
   * so that metrics on duration of the operations can
   * be distinguished.
   */
  public static final String OBJECT_BULK_DELETE_REQUEST
      = "object_bulk_delete_request";

  /**
   * A store's equivalent of a DELETE request was made: {@value}.
   * This may be an HTTP DELETE verb, or it may be some custom
   * operation which takes a list of objects to delete.
   */
  public static final String OBJECT_DELETE_REQUEST
      = "object_delete_request";

  /**
   * The count of objects deleted in delete requests.
   */
  public static final String OBJECT_DELETE_OBJECTS
      = "object_delete_objects";

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
   *
   * Value {@value}.
   */
  public static final String SUFFIX_MIN = ".min";

  /**
   * Suffix to use for a maximum value when
   * the same key is shared across max/mean/max
   * statistics.
   *
   * Value {@value}.
   */
  public static final String SUFFIX_MAX = ".max";

  /**
   * Suffix to use for a mean statistic value when
   * the same key is shared across mean/mean/max
   * statistics.
   *
   * Value {@value}.
   */
  public static final String SUFFIX_MEAN = ".mean";

  /**
   * String to add to counters and other stats to track failures.
   * This comes before the .min/.mean//max suffixes.
   *
   * Value {@value}.
   */
  public static final String SUFFIX_FAILURES = ".failures";

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

  /**
   * An HTTP DELETE request was made: {@value}.
   */
  public static final String ACTION_HTTP_DELETE_REQUEST
      = "action_http_delete_request";

  /**
   * An HTTP PUT request was made: {@value}.
   */
  public static final String ACTION_HTTP_PUT_REQUEST
      = "action_http_put_request";

  /**
   * An HTTP PATCH request was made: {@value}.
   */
  public static final String ACTION_HTTP_PATCH_REQUEST
      = "action_http_patch_request";

  /**
   * An HTTP POST request was made: {@value}.
   */
  public static final String ACTION_HTTP_POST_REQUEST
      = "action_http_post_request";

  /**
   * An HTTP HEAD request was made: {@value}.
   */
  public static final String OBJECT_METADATA_REQUESTS
      = "object_metadata_request";

  public static final String OBJECT_COPY_REQUESTS
      = "object_copy_requests";

  public static final String STORE_IO_THROTTLE_RATE
      = "store_io_throttle_rate";

  public static final String MULTIPART_UPLOAD_INSTANTIATED
      = "multipart_instantiated";

  public static final String MULTIPART_UPLOAD_PART_PUT
      = "multipart_upload_part_put";

  public static final String MULTIPART_UPLOAD_PART_PUT_BYTES
      = "multipart_upload_part_put_bytes";

  public static final String MULTIPART_UPLOAD_ABORTED
      = "multipart_upload_aborted";

  public static final String MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED
      = "multipart_upload_abort_under_path_invoked";

  public static final String MULTIPART_UPLOAD_COMPLETED
      = "multipart_upload_completed";

  public static final String MULTIPART_UPLOAD_STARTED
      = "multipart_upload_started";

  private StoreStatisticNames() {
  }

}
