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
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Config variable prefixes for ViewFs -
 *     see {@link org.apache.hadoop.fs.viewfs.ViewFs} for examples.
 * The mount table is specified in the config using these prefixes.
 * See {@link org.apache.hadoop.fs.viewfs.ConfigUtil} for convenience lib.
 */
public interface Constants {
  /**
   * Prefix for the config variable prefix for the ViewFs mount-table
   */
  public static final String CONFIG_VIEWFS_PREFIX = "fs.viewfs.mounttable";
 
  /**
   * Prefix for the home dir for the mount table - if not specified
   * then the hadoop default value (/user) is used.
   */
  public static final String CONFIG_VIEWFS_HOMEDIR = "homedir";
  
  /**
   * Config variable name for the default mount table.
   */
  public static final String CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE = "default";
  
  /**
   * Config variable full prefix for the default mount table.
   */
  public static final String CONFIG_VIEWFS_PREFIX_DEFAULT_MOUNT_TABLE = 
          CONFIG_VIEWFS_PREFIX + "." + CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;
  
  /**
   * Config variable for specifying a simple link
   */
  public static final String CONFIG_VIEWFS_LINK = "link";
  
  /**
   * Config variable for specifying a merge link
   */
  public static final String CONFIG_VIEWFS_LINK_MERGE = "linkMerge";

  /**
   * Config variable for specifying an nfly link. Nfly writes to multiple
   * locations, and allows reads from the closest one.
   */
  String CONFIG_VIEWFS_LINK_NFLY = "linkNfly";

  /**
   * Config variable for specifying a merge of the root of the mount-table
   *  with the root of another file system. 
   */
  public static final String CONFIG_VIEWFS_LINK_MERGE_SLASH = "linkMergeSlash";

  static public final FsPermission PERMISSION_555 =
      new FsPermission((short) 0555);

  String CONFIG_VIEWFS_RENAME_STRATEGY = "fs.viewfs.rename.strategy";
}
