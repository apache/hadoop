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
 *   <li>the name of the constants are uppercase, words separated by
 *   underscores.</li>
 *   <li>the value of the constants are lowercase of the constant names.</li>
 * </ul>
 */
@InterfaceStability.Unstable
public class StoreStatisticNames {

  /** {@value}. */
  public static final String OP_APPEND = "op_append";

  /** {@value}. */
  public static final String OP_COPY_FROM_LOCAL_FILE =
      "op_copy_from_local_file";

  /** {@value}. */
  public static final String OP_CREATE = "op_create";

  /** {@value}. */
  public static final String OP_CREATE_NON_RECURSIVE = "op_create_non_recursive";

  /** {@value}. */
  public static final String OP_DELETE = "op_delete";

  /** {@value}. */
  public static final String OP_EXISTS = "op_exists";

  /** {@value}. */
  public static final String OP_GET_CONTENT_SUMMARY = "op_get_content_summary";

  /** {@value}. */
  public static final String OP_GET_DELEGATION_TOKEN = "op_get_delegation_token";

  /** {@value}. */
  public static final String OP_GET_FILE_CHECKSUM = "op_get_file_checksum";

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
  public static final String OP_LIST_LOCATED_STATUS = "op_list_located_status";

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

  /** "Requests throttled and retried: {@value}. */
  public static final String STORE_IO_THROTTLED
      = "store_io_throttled";


}
