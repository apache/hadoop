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

package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;

/**
 * {@code DirListingMetadata} models a directory listing stored in a
 * {@link MetadataStore}.  Instances of this class are mutable and thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirListingMetadata {

  private final Path path;

  /** Using a map for fast find / remove with large directories. */
  private Map<Path, PathMetadata> listMap = new ConcurrentHashMap<>();

  private boolean isAuthoritative;

  /**
   * Create a directory listing metadata container.
   *
   * @param path Path of the directory.
   * @param listing Entries in the directory.
   * @param isAuthoritative true iff listing is the full contents of the
   *     directory, and the calling client reports that this may be cached as
   *     the full and authoritative listing of all files in the directory.
   */
  public DirListingMetadata(Path path, Collection<PathMetadata> listing,
      boolean isAuthoritative) {
    Preconditions.checkNotNull(path, "path must be non-null");
    this.path = path;
    if (listing != null) {
      for (PathMetadata entry : listing) {
        listMap.put(entry.getFileStatus().getPath(), entry);
      }
    }
    this.isAuthoritative  = isAuthoritative;
  }

  /**
   * @return {@code Path} of the directory that contains this listing.
   */
  public Path getPath() {
    return path;
  }

  /**
   * @return entries in the directory
   */
  public Collection<PathMetadata> getListing() {
    return Collections.unmodifiableCollection(listMap.values());
  }

  /**
   * @return true iff this directory listing is full and authoritative within
   * the scope of the {@code MetadataStore} that returned it.
   */
  public boolean isAuthoritative() {
    return isAuthoritative;
  }

  /**
   * Marks this directory listing as full and authoritative.
   */
  public void setAuthoritative() {
    isAuthoritative = true;
  }

  /**
   * Lookup entry within this directory listing.  This may return null if the
   * {@code MetadataStore} only tracks a partial set of the directory entries.
   * In the case where {@link #isAuthoritative()} is true, however, this
   * function returns null iff the directory is known not to contain the listing
   * at given path (within the scope of the {@code MetadataStore} that returned
   * it).
   *
   * @param childPath path of entry to look for.
   * @return entry, or null if it is not present or not being tracked.
   */
  public PathMetadata get(Path childPath) {
    checkChildPath(childPath);
    return listMap.get(childPath);
  }

  /**
   * Remove entry from this directory.
   *
   * @param childPath path of entry to remove.
   */
  public void remove(Path childPath) {
    checkChildPath(childPath);
    listMap.remove(childPath);
  }

  /**
   * Add an entry to the directory listing.  If this listing already contains a
   * {@code FileStatus} with the same path, it will be replaced.
   *
   * @param childFileStatus entry to add to this directory listing.
   */
  public void put(FileStatus childFileStatus) {
    Preconditions.checkNotNull(childFileStatus,
        "childFileStatus must be non-null");
    Path childPath = childFileStatus.getPath();
    checkChildPath(childPath);
    listMap.put(childPath, new PathMetadata(childFileStatus));
  }

  @Override
  public String toString() {
    return "DirListingMetadata{" +
        "path=" + path +
        ", listMap=" + listMap +
        ", isAuthoritative=" + isAuthoritative +
        '}';
  }

  /**
   * Performs pre-condition checks on child path arguments.
   *
   * @param childPath path to check.
   */
  private void checkChildPath(Path childPath) {
    Preconditions.checkNotNull(childPath, "childPath must be non-null");
    Preconditions.checkArgument(!childPath.isRoot(),
        "childPath cannot be the root path");
    Preconditions.checkArgument(childPath.getParent().equals(path),
        "childPath must be a child of path");
  }
}
