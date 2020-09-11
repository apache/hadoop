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
   * Prefix for the config variable for the ViewFs mount-table path.
   */
  String CONFIG_VIEWFS_MOUNTTABLE_PATH = CONFIG_VIEWFS_PREFIX + ".path";
 
  /**
   * Prefix for the home dir for the mount table - if not specified
   * then the hadoop default value (/user) is used.
   */
  public static final String CONFIG_VIEWFS_HOMEDIR = "homedir";

  /**
   * Config key to specify the name of the default mount table.
   */
  String CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE_NAME_KEY =
      "fs.viewfs.mounttable.default.name.key";

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
  String CONFIG_VIEWFS_LINK = "link";

  /**
   * Config variable for specifying a fallback for link mount points.
   */
  String CONFIG_VIEWFS_LINK_FALLBACK = "linkFallback";

  /**
   * Config variable for specifying a merge link
   */
  String CONFIG_VIEWFS_LINK_MERGE = "linkMerge";

  /**
   * Config variable for specifying an nfly link. Nfly writes to multiple
   * locations, and allows reads from the closest one.
   */
  String CONFIG_VIEWFS_LINK_NFLY = "linkNfly";

  /**
   * Config variable for specifying a merge of the root of the mount-table
   *  with the root of another file system. 
   */
  String CONFIG_VIEWFS_LINK_MERGE_SLASH = "linkMergeSlash";

  /**
   * Config variable for specifying a regex link which uses regular expressions
   * as source and target could use group captured in src.
   * E.g. (^/(?<firstDir>\\w+), /prefix-${firstDir}) =>
   *   (/path1/file1 => /prefix-path1/file1)
   */
  String CONFIG_VIEWFS_LINK_REGEX = "linkRegex";

  FsPermission PERMISSION_555 = new FsPermission((short) 0555);

  String CONFIG_VIEWFS_RENAME_STRATEGY = "fs.viewfs.rename.strategy";

  /**
   * Enable ViewFileSystem to cache all children filesystems in inner cache.
   */
  String CONFIG_VIEWFS_ENABLE_INNER_CACHE = "fs.viewfs.enable.inner.cache";

  boolean CONFIG_VIEWFS_ENABLE_INNER_CACHE_DEFAULT = true;

  /**
   * Enable ViewFileSystem to show mountlinks as symlinks.
   */
  String CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS =
      "fs.viewfs.mount.links.as.symlinks";

  boolean CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS_DEFAULT = true;

  /**
   * When initializing the viewfs, authority will be used as the mount table
   * name to find the mount link configurations. To make the mount table name
   * unique, we may want to ignore port if initialized uri authority contains
   * port number. By default, we will consider port number also in
   * ViewFileSystem(This default value false, because to support existing
   * deployments continue with the current behavior).
   */
  String CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME =
      "fs.viewfs.ignore.port.in.mount.table.name";

  boolean CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME_DEFAULT = false;
}
