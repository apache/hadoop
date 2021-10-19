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

import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * FileSystem related constants.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface FsConstants {
  // URI for local filesystem
  public static final URI LOCAL_FS_URI = URI.create("file:///");
  
  // URI scheme for FTP
  public static final String FTP_SCHEME = "ftp";

  // Maximum number of symlinks to recursively resolve in a path
  static final int MAX_PATH_LINKS = 32;

  /**
   * ViewFs: viewFs file system (ie the mount file system on client side)
   */
  public static final URI VIEWFS_URI = URI.create("viewfs:///");
  public static final String VIEWFS_SCHEME = "viewfs";
  String FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN =
      "fs.viewfs.overload.scheme.target.%s.impl";
  String VIEWFS_TYPE = "viewfs";
}
