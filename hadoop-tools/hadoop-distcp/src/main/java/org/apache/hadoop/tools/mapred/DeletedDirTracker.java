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

package org.apache.hadoop.tools.mapred;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.CopyListingFileStatus;

/**
 * Track deleted directories and support queries to
 * check for add them.
 *
 * Assumptions.
 * <ol>
 *   <liA sorted list of deletions are processed, where directories come
 *   before their children/descendants.</li>
 *   <li>Deep directory trees are being deleted.</li>
 *   <li>The total number of directories deleted is very much
 *   less than the number of files.</li>
 *   <li>Most deleted files are in directories which have
 *   been deleted.</li>
 *   <li>The cost of issuing a delete() call is less than that that
 *   of creating Path entries for parent directories and looking them
 *   up in a hash table.</li>
 *   <li>That a modest cache is sufficient to identify whether or not
 *   a parent directory has been deleted./li>
 *   <li>And that if a path has been evicted from a path, the cost of
 *   the extra deletions incurred is not significant.</li>
 * </ol>
 *
 * The directory structure this algorithm is intended to optimize for is
 * the deletion of datasets partitioned/bucketed into a directory tree,
 * and deleted in bulk.
 *
 * The ordering of deletions comes from the merge sort of the copy listings;
 * we rely on this placing a path "/dir1" ahead of "/dir1/file1",
 * "/dir1/dir2/file2", and other descendants.
 * We do not rely on parent entries being added immediately before children,
 * as sorting may place "/dir12" between "/dir1" and its descendants.
 *
 * Algorithm
 *
 * <ol>
 *   <li>
 *     Before deleting a directory or file, a check is made to see if an
 *     ancestor is in the cache of deleted directories.
 *   </li>
 *   <li>
 *     If an ancestor is found is: skip the delete.
 *   </li>
 *   <li>
 *     If an ancestor is not foundI: delete the file/dir.
 *   </li>
 *   <li>
 *     When the entry probed is a directory, it is always added to the cache of
 *     directories, irrespective of the search for an ancestor.
 *     This is to speed up scans of files directly underneath the path.
 *   </li>
 * </ol>
 *
 *
 */
final class DeletedDirTracker {

  /**
   * An LRU cache of directories.
   */
  private final Cache<Path, Path> directories;

  /**
   * Maximum size of the cache.
   */
  private final int cacheSize;

  /**
   * Create an instance.
   * @param cacheSize maximum cache size.
   */
  DeletedDirTracker(int cacheSize) {
    this.cacheSize = cacheSize;
    directories = CacheBuilder.newBuilder()
        .maximumSize(this.cacheSize)
        .build();
  }

  /**
   * Recursive scan for a directory being in the cache of deleted paths.
   * @param dir directory to look for.
   * @return true iff the path or a parent is in the cache.
   */
  boolean isDirectoryOrAncestorDeleted(Path dir) {
    if (dir == null) {
      // at root
      return false;
    } else if (isContained(dir)) {
      // cache hit
      return true;
    } else {
      // cache miss, check parent
      return isDirectoryOrAncestorDeleted(dir.getParent());
    }
  }

  /**
   * Probe for a path being deleted by virtue of the fact that an
   * ancestor dir has already been deleted.
   * @param path path to check
   * @return true if the parent dir is deleted.
   */
  private boolean isInDeletedDirectory(Path path) {
    Preconditions.checkArgument(!path.isRoot(), "Root Dir");
    return isDirectoryOrAncestorDeleted(path.getParent());
  }

  /**
   * Should a file or directory be deleted?
   * The cache of deleted directories will be updated with the path
   * of the status if it references a directory.
   * @param status file/path to check
   * @return true if the path should be deleted.
   */
  boolean shouldDelete(CopyListingFileStatus status) {
    Path path = status.getPath();
    Preconditions.checkArgument(!path.isRoot(), "Root Dir");
    if (status.isDirectory()) {
      boolean deleted = isDirectoryOrAncestorDeleted(path);
      // even if an ancestor has been deleted, add this entry as
      // a deleted directory.
      directories.put(path, path);
      return !deleted;
    } else {
      return !isInDeletedDirectory(path);
    }
  }

  /**
   * Is a path directly contained in the set of deleted directories.
   * @param dir directory to probe
   * @return true if this directory is recorded as being deleted.
   */
  boolean isContained(Path dir) {
    return directories.getIfPresent(dir) != null;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "DeletedDirTracker{");
    sb.append("maximum size=").append(cacheSize);
    sb.append("; current size=").append(directories.size());
    sb.append('}');
    return sb.toString();
  }

  /**
   * Return the current size of the tracker, as in #of entries in the cache.
   * @return tracker size.
   */
  long size() {
    return directories.size();
  }
}
