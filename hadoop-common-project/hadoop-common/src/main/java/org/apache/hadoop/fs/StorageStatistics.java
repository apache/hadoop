/**
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
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Iterator;

/**
 * StorageStatistics contains statistics data for a FileSystem or FileContext
 * instance.
 */
@InterfaceAudience.Public
public abstract class StorageStatistics {

  /**
   * These are common statistic names.
   *
   * The following names are considered general and preserved across different
   * StorageStatistics classes. When implementing a new StorageStatistics, it is
   * highly recommended to use the common statistic names.
   *
   * When adding new common statistic name constants, please make them unique.
   * By convention, they are implicitly unique:
   *  - the name of the constants are uppercase, words separated by underscores.
   *  - the value of the constants are lowercase of the constant names.
   */
  public interface CommonStatisticNames {
    // The following names are for file system operation invocations
    String OP_APPEND = "op_append";
    String OP_COPY_FROM_LOCAL_FILE = "op_copy_from_local_file";
    String OP_CREATE = "op_create";
    String OP_CREATE_NON_RECURSIVE = "op_create_non_recursive";
    String OP_DELETE = "op_delete";
    String OP_EXISTS = "op_exists";
    String OP_GET_CONTENT_SUMMARY = "op_get_content_summary";
    String OP_GET_FILE_CHECKSUM = "op_get_file_checksum";
    String OP_GET_FILE_STATUS = "op_get_file_status";
    String OP_GET_STATUS = "op_get_status";
    String OP_GLOB_STATUS = "op_glob_status";
    String OP_IS_FILE = "op_is_file";
    String OP_IS_DIRECTORY = "op_is_directory";
    String OP_LIST_FILES = "op_list_files";
    String OP_LIST_LOCATED_STATUS = "op_list_located_status";
    String OP_LIST_STATUS = "op_list_status";
    String OP_MKDIRS = "op_mkdirs";
    String OP_MODIFY_ACL_ENTRIES = "op_modify_acl_entries";
    String OP_OPEN = "op_open";
    String OP_REMOVE_ACL = "op_remove_acl";
    String OP_REMOVE_ACL_ENTRIES = "op_remove_acl_entries";
    String OP_REMOVE_DEFAULT_ACL = "op_remove_default_acl";
    String OP_RENAME = "op_rename";
    String OP_SET_ACL = "op_set_acl";
    String OP_SET_OWNER = "op_set_owner";
    String OP_SET_PERMISSION = "op_set_permission";
    String OP_SET_TIMES = "op_set_times";
    String OP_TRUNCATE = "op_truncate";
  }

  /**
   * A 64-bit storage statistic.
   */
  public static class LongStatistic {
    private final String name;
    private final long value;

    public LongStatistic(String name, long value) {
      this.name = name;
      this.value = value;
    }

    /**
     * @return    The name of this statistic.
     */
    public String getName() {
      return name;
    }

    /**
     * @return    The value of this statistic.
     */
    public long getValue() {
      return value;
    }

    @Override
    public String toString() {
      return name + " = " + value;
    }
  }

  private final String name;

  public StorageStatistics(String name) {
    this.name = name;
  }

  /**
   * Get the name of this StorageStatistics object.
   */
  public String getName() {
    return name;
  }

  /**
   * @return the associated file system scheme if this is scheme specific,
   * else return null.
   */
  public String getScheme() {
    return null;
  }

  /**
   * Get an iterator over all the currently tracked long statistics.
   *
   * The values returned will depend on the type of FileSystem or FileContext
   * object.  The values do not necessarily reflect a snapshot in time.
   */
  public abstract Iterator<LongStatistic> getLongStatistics();

  /**
   * Get the value of a statistic.
   *
   * @return         null if the statistic is not being tracked or is not a
   *                 long statistic. The value of the statistic, otherwise.
   */
  public abstract Long getLong(String key);

  /**
   * Return true if a statistic is being tracked.
   *
   * @return         True only if the statistic is being tracked.
   */
  public abstract boolean isTracked(String key);

  /**
   * Reset all the statistic data.
   */
  public abstract void reset();
}
