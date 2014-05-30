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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.annotations.VisibleForTesting;

/**
 * {@link LocalCacheDirectoryManager} is used for managing hierarchical
 * directories for local cache. It will allow to restrict the number of files in
 * a directory to
 * {@link YarnConfiguration#NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY} which
 * includes 36 sub-directories (named from 0 to 9 and a to z). Root directory is
 * represented by an empty string. It internally maintains a vacant directory
 * queue. As soon as the file count for the directory reaches its limit; new
 * files will not be created in it until at least one file is deleted from it.
 * New sub directories are not created unless a
 * {@link LocalCacheDirectoryManager#getRelativePathForLocalization()} request
 * is made and nonFullDirectories are empty.
 * 
 * Note : this structure only returns relative localization path but doesn't
 * create one on disk.
 */
public class LocalCacheDirectoryManager {

  private final int perDirectoryFileLimit;
  // total 36 = a to z plus 0 to 9
  public static final int DIRECTORIES_PER_LEVEL = 36;

  private Queue<Directory> nonFullDirectories;
  private HashMap<String, Directory> knownDirectories;
  private int totalSubDirectories;

  public LocalCacheDirectoryManager(Configuration conf) {
    totalSubDirectories = 0;
    Directory rootDir = new Directory(totalSubDirectories);
    nonFullDirectories = new LinkedList<Directory>();
    knownDirectories = new HashMap<String, Directory>();
    knownDirectories.put("", rootDir);
    nonFullDirectories.add(rootDir);
    this.perDirectoryFileLimit =
        conf.getInt(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY,
          YarnConfiguration.DEFAULT_NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY) - 36;
  }

  /**
   * This method will return relative path from the first available vacant
   * directory.
   * 
   * @return {@link String} relative path for localization
   */
  public synchronized String getRelativePathForLocalization() {
    if (nonFullDirectories.isEmpty()) {
      totalSubDirectories++;
      Directory newDir = new Directory(totalSubDirectories);
      nonFullDirectories.add(newDir);
      knownDirectories.put(newDir.getRelativePath(), newDir);
    }
    Directory subDir = nonFullDirectories.peek();
    if (subDir.incrementAndGetCount() >= perDirectoryFileLimit) {
      nonFullDirectories.remove();
    }
    return subDir.getRelativePath();
  }

  /**
   * This method will reduce the file count for the directory represented by
   * path. The root directory of this Local cache directory manager is
   * represented by an empty string.
   */
  public synchronized void decrementFileCountForPath(String relPath) {
    relPath = relPath == null ? "" : relPath.trim();
    Directory subDir = knownDirectories.get(relPath);
    int oldCount = subDir.getCount();
    if (subDir.decrementAndGetCount() < perDirectoryFileLimit
        && oldCount >= perDirectoryFileLimit) {
      nonFullDirectories.add(subDir);
    }
  }

  /**
   * Increment the file count for a relative directory within the cache
   * 
   * @param relPath the relative path
   */
  public synchronized void incrementFileCountForPath(String relPath) {
    relPath = relPath == null ? "" : relPath.trim();
    Directory subDir = knownDirectories.get(relPath);
    if (subDir == null) {
      int dirnum = Directory.getDirectoryNumber(relPath);
      totalSubDirectories = Math.max(dirnum, totalSubDirectories);
      subDir = new Directory(dirnum);
      nonFullDirectories.add(subDir);
      knownDirectories.put(subDir.getRelativePath(), subDir);
    }
    if (subDir.incrementAndGetCount() >= perDirectoryFileLimit) {
      nonFullDirectories.remove(subDir);
    }
  }

  /**
   * Given a path to a directory within a local cache tree return the
   * root of the cache directory.
   * 
   * @param path the directory within a cache directory
   * @return the local cache directory root or null if not found
   */
  public static Path getCacheDirectoryRoot(Path path) {
    while (path != null) {
      String name = path.getName();
      if (name.length() != 1) {
        return path;
      }
      int dirnum = DIRECTORIES_PER_LEVEL;
      try {
        dirnum = Integer.parseInt(name, DIRECTORIES_PER_LEVEL);
      } catch (NumberFormatException e) {
      }
      if (dirnum >= DIRECTORIES_PER_LEVEL) {
        return path;
      }
      path = path.getParent();
    }
    return path;
  }

  @VisibleForTesting
  synchronized Directory getDirectory(String relPath) {
    return knownDirectories.get(relPath);
  }

  /*
   * It limits the number of files and sub directories in the directory to the
   * limit LocalCacheDirectoryManager#perDirectoryFileLimit.
   */
  static class Directory {

    private final String relativePath;
    private int fileCount;

    static String getRelativePath(int directoryNo) {
      String relativePath = "";
      if (directoryNo > 0) {
        String tPath = Integer.toString(directoryNo - 1, DIRECTORIES_PER_LEVEL);
        StringBuffer sb = new StringBuffer();
        if (tPath.length() == 1) {
          sb.append(tPath.charAt(0));
        } else {
          // this is done to make sure we also reuse 0th sub directory
          sb.append(Integer.toString(
            Integer.parseInt(tPath.substring(0, 1), DIRECTORIES_PER_LEVEL) - 1,
            DIRECTORIES_PER_LEVEL));
        }
        for (int i = 1; i < tPath.length(); i++) {
          sb.append(Path.SEPARATOR).append(tPath.charAt(i));
        }
        relativePath = sb.toString();
      }
      return relativePath;
    }

    static int getDirectoryNumber(String relativePath) {
      String numStr = relativePath.replace("/", "");
      if (relativePath.isEmpty()) {
        return 0;
      }
      if (numStr.length() > 1) {
        // undo step from getRelativePath() to reuse 0th sub directory
        String firstChar = Integer.toString(
            Integer.parseInt(numStr.substring(0, 1),
                DIRECTORIES_PER_LEVEL) + 1, DIRECTORIES_PER_LEVEL);
        numStr = firstChar + numStr.substring(1);
      }
      return Integer.parseInt(numStr, DIRECTORIES_PER_LEVEL) + 1;
    }

    public Directory(int directoryNo) {
      fileCount = 0;
      relativePath = getRelativePath(directoryNo);
    }

    public int incrementAndGetCount() {
      return ++fileCount;
    }

    public int decrementAndGetCount() {
      return --fileCount;
    }

    public String getRelativePath() {
      return relativePath;
    }

    public int getCount() {
      return fileCount;
    }
  }
}