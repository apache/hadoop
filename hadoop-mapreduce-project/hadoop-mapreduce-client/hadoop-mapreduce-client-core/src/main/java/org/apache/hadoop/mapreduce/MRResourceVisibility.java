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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

/**
 * This enum is used to represent visibility of MapReduce resource.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum MRResourceVisibility {
  // DirPermision, FilePermission
  PUBLIC(FsPermission.createImmutable((short) 0755),
      FsPermission.createImmutable((short) 0755), "public"),
  PRIVATE(FsPermission.createImmutable((short) 0750),
      FsPermission.createImmutable((short) 0750), "private"),
  APPLICATION(FsPermission.createImmutable((short) 0700),
      FsPermission.createImmutable((short) 0700), "application");

  private FsPermission dirPermission;
  private FsPermission filePermission;
  // The parent dir name for resources.
  private String dirName;

  MRResourceVisibility(
      FsPermission dirPerm, FsPermission filePerm, String dirNam) {
    dirPermission = dirPerm;
    filePermission = filePerm;
    dirName = dirNam;
  }

  public FsPermission getDirPermission() {
    return dirPermission;
  }

  public FsPermission getFilePermission() {
    return filePermission;
  }

  public String getDirName() {
    return dirName;
  }

  public static boolean contains(String visibility) {
    if (visibility == null) {
      return false;
    }
    visibility = visibility.toUpperCase();
    for (MRResourceVisibility v : MRResourceVisibility.values()) {
      if (v.name().equals(visibility)) {
        return true;
      }
    }
    return false;
  }

  public static MRResourceVisibility getVisibility(String visibilityStr) {
    if (visibilityStr == null) {
      return null;
    }
    visibilityStr = visibilityStr.toUpperCase();
    for (MRResourceVisibility v : MRResourceVisibility.values()) {
      if (v.name().equals(visibilityStr)) {
        return v;
      }
    }
    return null;
  }

  public static LocalResourceVisibility getYarnLocalResourceVisibility(
      MRResourceVisibility visibility) {
    switch (visibility) {
    case PUBLIC:
      return LocalResourceVisibility.PUBLIC;
    case PRIVATE:
      return LocalResourceVisibility.PRIVATE;
    case APPLICATION:
      return LocalResourceVisibility.APPLICATION;
    default:
      throw new IllegalArgumentException(
          "Unsupport resourceVisility " + visibility.name());
    }
  }
}
