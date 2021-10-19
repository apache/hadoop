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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.viewfs.ViewFileSystem.MountPoint;

/**
 * Utility APIs for ViewFileSystem.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ViewFileSystemUtil {

  private ViewFileSystemUtil() {
    // Private Constructor
  }

  /**
   * Check if the FileSystem is a ViewFileSystem.
   *
   * @param fileSystem
   * @return true if the fileSystem is ViewFileSystem
   */
  public static boolean isViewFileSystem(final FileSystem fileSystem) {
    return fileSystem.getScheme().equals(FsConstants.VIEWFS_SCHEME);
  }

  /**
   * Check if the FileSystem is a ViewFileSystemOverloadScheme.
   *
   * @param fileSystem
   * @return true if the fileSystem is ViewFileSystemOverloadScheme
   */
  public static boolean isViewFileSystemOverloadScheme(
      final FileSystem fileSystem) {
    return fileSystem instanceof ViewFileSystemOverloadScheme;
  }

  /**
   * Get FsStatus for all ViewFsMountPoints matching path for the given
   * ViewFileSystem.
   *
   * Say ViewFileSystem has following mount points configured
   *  (1) hdfs://NN0_host:port/sales mounted on /dept/sales
   *  (2) hdfs://NN1_host:port/marketing mounted on /dept/marketing
   *  (3) hdfs://NN2_host:port/eng_usa mounted on /dept/eng/usa
   *  (4) hdfs://NN3_host:port/eng_asia mounted on /dept/eng/asia
   *
   * For the above config, here is a sample list of paths and their matching
   * mount points while getting FsStatus
   *
   *  Path                  Description                      Matching MountPoint
   *
   *  "/"                   Root ViewFileSystem lists all    (1), (2), (3), (4)
   *                         mount points.
   *
   *  "/dept"               Not a mount point, but a valid   (1), (2), (3), (4)
   *                         internal dir in the mount tree
   *                         and resolved down to "/" path.
   *
   *  "/dept/sales"         Matches a mount point            (1)
   *
   *  "/dept/sales/india"   Path is over a valid mount point (1)
   *                         and resolved down to
   *                         "/dept/sales"
   *
   *  "/dept/eng"           Not a mount point, but a valid   (1), (2), (3), (4)
   *                         internal dir in the mount tree
   *                         and resolved down to "/" path.
   *
   *  "/erp"                Doesn't match or leads to or
   *                         over any valid mount points     None
   *
   *
   * @param fileSystem - ViewFileSystem on which mount point exists
   * @param path - URI for which FsStatus is requested
   * @return Map of ViewFsMountPoint and FsStatus
   */
  public static Map<MountPoint, FsStatus> getStatus(
      FileSystem fileSystem, Path path) throws IOException {
    if (!(isViewFileSystem(fileSystem)
        || isViewFileSystemOverloadScheme(fileSystem))) {
      throw new UnsupportedFileSystemException("FileSystem '"
          + fileSystem.getUri() + "'is not a ViewFileSystem.");
    }
    ViewFileSystem viewFileSystem = (ViewFileSystem) fileSystem;
    String viewFsUriPath = viewFileSystem.getUriPath(path);
    boolean isPathOverMountPoint = false;
    boolean isPathLeadingToMountPoint = false;
    boolean isPathIncludesAllMountPoint = false;
    Map<MountPoint, FsStatus> mountPointMap = new HashMap<>();
    for (MountPoint mountPoint : viewFileSystem.getMountPoints()) {
      String[] mountPointPathComponents = InodeTree.breakIntoPathComponents(
          mountPoint.getMountedOnPath().toString());
      String[] incomingPathComponents =
          InodeTree.breakIntoPathComponents(viewFsUriPath);

      int pathCompIndex;
      for (pathCompIndex = 0; pathCompIndex < mountPointPathComponents.length &&
          pathCompIndex < incomingPathComponents.length; pathCompIndex++) {
        if (!mountPointPathComponents[pathCompIndex].equals(
            incomingPathComponents[pathCompIndex])) {
          break;
        }
      }

      if (pathCompIndex >= mountPointPathComponents.length) {
        // Patch matches or over a valid mount point
        isPathOverMountPoint = true;
        mountPointMap.clear();
        updateMountPointFsStatus(viewFileSystem, mountPointMap, mountPoint,
            new Path(viewFsUriPath));
        break;
      } else {
        if (pathCompIndex > 1) {
          // Path is in the mount tree
          isPathLeadingToMountPoint = true;
        } else if (incomingPathComponents.length <= 1) {
          // Special case of "/" path
          isPathIncludesAllMountPoint = true;
        }
        updateMountPointFsStatus(viewFileSystem, mountPointMap, mountPoint,
            mountPoint.getMountedOnPath());
      }
    }

    if (!isPathOverMountPoint && !isPathLeadingToMountPoint &&
        !isPathIncludesAllMountPoint) {
      throw new NotInMountpointException(path, "getStatus");
    }
    return mountPointMap;
  }

  /**
   * Update FsStatus for the given the mount point.
   *
   * @param viewFileSystem
   * @param mountPointMap
   * @param mountPoint
   * @param path
   */
  private static void updateMountPointFsStatus(
      final ViewFileSystem viewFileSystem,
      final Map<MountPoint, FsStatus> mountPointMap,
      final MountPoint mountPoint, final Path path) throws IOException {
    FsStatus fsStatus = viewFileSystem.getStatus(path);
    mountPointMap.put(mountPoint, fsStatus);
  }

}
