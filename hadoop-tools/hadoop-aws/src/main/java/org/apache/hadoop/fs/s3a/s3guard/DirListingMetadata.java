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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.s3a.Tristate;

/**
 * {@code DirListingMetadata} models a directory listing stored in a
 * {@link MetadataStore}.  Instances of this class are mutable and thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirListingMetadata {

  /**
   * Convenience parameter for passing into constructor.
   */
  public static final Collection<PathMetadata> EMPTY_DIR =
      Collections.emptyList();

  private final Path path;

  /** Using a map for fast find / remove with large directories. */
  private Map<Path, PathMetadata> listMap = new ConcurrentHashMap<>();

  private boolean isAuthoritative;

  /**
   * Create a directory listing metadata container.
   *
   * @param path Path of the directory. If this path has a host component, then
   *     all paths added later via put() must also have the same host.
   * @param listing Entries in the directory.
   * @param isAuthoritative true iff listing is the full contents of the
   *     directory, and the calling client reports that this may be cached as
   *     the full and authoritative listing of all files in the directory.
   */
  public DirListingMetadata(Path path, Collection<PathMetadata> listing,
      boolean isAuthoritative) {

    checkPathAbsolute(path);
    this.path = path;

    if (listing != null) {
      for (PathMetadata entry : listing) {
        Path childPath = entry.getFileStatus().getPath();
        checkChildPath(childPath);
        listMap.put(childPath, entry);
      }
    }
    this.isAuthoritative  = isAuthoritative;
  }

  /**
   * Copy constructor.
   * @param d the existing {@link DirListingMetadata} object.
   */
  public DirListingMetadata(DirListingMetadata d) {
    path = d.path;
    isAuthoritative = d.isAuthoritative;
    listMap = new ConcurrentHashMap<>(d.listMap);
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
   * @return number of entries tracked.  This is not the same as the number
   * of entries in the actual directory unless {@link #isAuthoritative()} is
   * true.
   */
  public int numEntries() {
    return listMap.size();
  }

  /**
   * @return true iff this directory listing is full and authoritative within
   * the scope of the {@code MetadataStore} that returned it.
   */
  public boolean isAuthoritative() {
    return isAuthoritative;
  }


  /**
   * Is the underlying directory known to be empty?
   * @return FALSE if directory is known to have a child entry, TRUE if
   * directory is known to be empty, UNKNOWN otherwise.
   */
  public Tristate isEmpty() {
    if (getListing().isEmpty()) {
      if (isAuthoritative()) {
        return Tristate.TRUE;
      } else {
        // This listing is empty, but may not be full list of underlying dir.
        return Tristate.UNKNOWN;
      }
    } else { // not empty listing
      // There exists at least one child, dir not empty.
      return Tristate.FALSE;
    }
  }

  /**
   * Marks this directory listing as full and authoritative.
   * @param authoritative see {@link #isAuthoritative()}.
   */
  public void setAuthoritative(boolean authoritative) {
    this.isAuthoritative = authoritative;
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
   * @return true if the status was added or replaced with a new value. False
   * if the same FileStatus value was already present.
   */
  public boolean put(FileStatus childFileStatus) {
    Preconditions.checkNotNull(childFileStatus,
        "childFileStatus must be non-null");
    Path childPath = childStatusToPathKey(childFileStatus);
    PathMetadata newValue = new PathMetadata(childFileStatus);
    PathMetadata oldValue = listMap.put(childPath, newValue);
    if (oldValue == null) {
      return true;
    } else {
      return !oldValue.equals(newValue);
    }
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
   * Log contents to supplied StringBuilder in a pretty fashion.
   * @param sb target StringBuilder
   */
  public void prettyPrint(StringBuilder sb) {
    sb.append(String.format("DirMeta %-20s %-18s",
        path.toString(),
        isAuthoritative ? "Authoritative" : "Not Authoritative"));
    for (Map.Entry<Path, PathMetadata> entry : listMap.entrySet()) {
      sb.append("\n   key: ").append(entry.getKey()).append(": ");
      entry.getValue().prettyPrint(sb);
    }
    sb.append("\n");
  }

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    prettyPrint(sb);
    return sb.toString();
  }
  /**
   * Checks that child path is valid.
   * @param childPath path to check.
   */
  private void checkChildPath(Path childPath) {
    checkPathAbsolute(childPath);

    // If this dir's path has host (and thus scheme), so must its children
    URI parentUri = path.toUri();
    if (parentUri.getHost() != null) {
      URI childUri = childPath.toUri();
      Preconditions.checkNotNull(childUri.getHost(), "Expected non-null URI " +
          "host: %s", childUri);
      Preconditions.checkArgument(
          childUri.getHost().equals(parentUri.getHost()),
          "childUri %s and parentUri %s should have the same host",
          childUri, parentUri);
      Preconditions.checkNotNull(childUri.getScheme());
    }
    Preconditions.checkArgument(!childPath.isRoot(),
        "childPath cannot be the root path");
    Preconditions.checkArgument(childPath.getParent().equals(path),
        "childPath %s must be a child of %s", childPath, path);
  }

  /**
   * For Path's that are handed in directly, we assert they are in consistent
   * format with checkPath().  For paths that are supplied embedded in
   * FileStatus', we attempt to fill in missing scheme and host, when this
   * DirListingMetadata is associated with one.
   *
   * @return Path suitable for consistent hashtable lookups
   */
  private Path childStatusToPathKey(FileStatus status) {
    Path p = status.getPath();
    Preconditions.checkNotNull(p, "Child status' path cannot be null");
    Preconditions.checkArgument(!p.isRoot(),
        "childPath cannot be the root path");
    Preconditions.checkArgument(p.getParent().equals(path),
        "childPath must be a child of path");
    URI uri = p.toUri();
    URI parentUri = path.toUri();
    // If FileStatus' path is missing host, but should have one, add it.
    if (uri.getHost() == null && parentUri.getHost() != null) {
      try {
        return new Path(new URI(parentUri.getScheme(), parentUri.getHost(),
            uri.getPath(), uri.getFragment()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("FileStatus path invalid with " +
            " added " + parentUri.getScheme() + "://" + parentUri.getHost() +
            " added", e);
      }
    }
    return p;
  }

  private void checkPathAbsolute(Path p) {
    Preconditions.checkNotNull(p, "path must be non-null");
    Preconditions.checkArgument(p.isAbsolute(), "path must be absolute");
  }
}
