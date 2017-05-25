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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Tristate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * This is a local, in-memory, implementation of MetadataStore.
 * This is *not* a coherent cache across processes.  It is only
 * locally-coherent.
 *
 * The purpose of this is for unit testing.  It could also be used to accelerate
 * local-only operations where only one process is operating on a given object
 * store, or multiple processes are accessing a read-only storage bucket.
 *
 * This MetadataStore does not enforce filesystem rules such as disallowing
 * non-recursive removal of non-empty directories.  It is assumed the caller
 * already has to perform these sorts of checks.
 */
public class LocalMetadataStore implements MetadataStore {

  public static final Logger LOG = LoggerFactory.getLogger(MetadataStore.class);
  // TODO HADOOP-13649: use time instead of capacity for eviction.
  public static final int DEFAULT_MAX_RECORDS = 128;
  public static final String CONF_MAX_RECORDS =
      "fs.metadatastore.local.max_records";

  /** Contains directories and files. */
  private LruHashMap<Path, PathMetadata> fileHash;

  /** Contains directory listings. */
  private LruHashMap<Path, DirListingMetadata> dirHash;

  private FileSystem fs;
  /* Null iff this FS does not have an associated URI host. */
  private String uriHost;

  @Override
  public void initialize(FileSystem fileSystem) throws IOException {
    Preconditions.checkNotNull(fileSystem);
    fs = fileSystem;
    URI fsURI = fs.getUri();
    uriHost = fsURI.getHost();
    if (uriHost != null && uriHost.equals("")) {
      uriHost = null;
    }

    initialize(fs.getConf());
  }

  @Override
  public void initialize(Configuration conf) throws IOException {
    Preconditions.checkNotNull(conf);
    int maxRecords = conf.getInt(CONF_MAX_RECORDS, DEFAULT_MAX_RECORDS);
    if (maxRecords < 4) {
      maxRecords = 4;
    }
    // Start w/ less than max capacity.  Space / time trade off.
    fileHash = new LruHashMap<>(maxRecords/2, maxRecords);
    dirHash = new LruHashMap<>(maxRecords/4, maxRecords);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "LocalMetadataStore{");
    sb.append(", uriHost='").append(uriHost).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public void delete(Path p) throws IOException {
    doDelete(p, false, true);
  }

  @Override
  public void forgetMetadata(Path p) throws IOException {
    doDelete(p, false, false);
  }

  @Override
  public void deleteSubtree(Path path) throws IOException {
    doDelete(path, true, true);
  }

  private synchronized void doDelete(Path p, boolean recursive, boolean
      tombstone) {

    Path path = standardize(p);

    // Delete entry from file cache, then from cached parent directory, if any

    deleteHashEntries(path, tombstone);

    if (recursive) {
      // Remove all entries that have this dir as path prefix.
      deleteHashByAncestor(path, dirHash, tombstone);
      deleteHashByAncestor(path, fileHash, tombstone);
    }
  }

  @Override
  public synchronized PathMetadata get(Path p) throws IOException {
    return get(p, false);
  }

  @Override
  public PathMetadata get(Path p, boolean wantEmptyDirectoryFlag)
      throws IOException {
    Path path = standardize(p);
    synchronized (this) {
      PathMetadata m = fileHash.mruGet(path);

      if (wantEmptyDirectoryFlag && m != null &&
          m.getFileStatus().isDirectory()) {
        m.setIsEmptyDirectory(isEmptyDirectory(p));
      }

      LOG.debug("get({}) -> {}", path, m == null ? "null" : m.prettyPrint());
      return m;
    }
  }

  /**
   * Determine if directory is empty.
   * Call with lock held.
   * @param p a Path, already filtered through standardize()
   * @return TRUE / FALSE if known empty / not-empty, UNKNOWN otherwise.
   */
  private Tristate isEmptyDirectory(Path p) {
    DirListingMetadata dirMeta = dirHash.get(p);
    return dirMeta.withoutTombstones().isEmpty();
  }

  @Override
  public synchronized DirListingMetadata listChildren(Path p) throws
      IOException {
    Path path = standardize(p);
    DirListingMetadata listing = dirHash.mruGet(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("listChildren({}) -> {}", path,
          listing == null ? "null" : listing.prettyPrint());
    }
    // Make a copy so callers can mutate without affecting our state
    return listing == null ? null : new DirListingMetadata(listing);
  }

  @Override
  public void move(Collection<Path> pathsToDelete,
      Collection<PathMetadata> pathsToCreate) throws IOException {

    Preconditions.checkNotNull(pathsToDelete, "pathsToDelete is null");
    Preconditions.checkNotNull(pathsToCreate, "pathsToCreate is null");
    Preconditions.checkArgument(pathsToDelete.size() == pathsToCreate.size(),
        "Must supply same number of paths to delete/create.");

    // I feel dirty for using reentrant lock. :-|
    synchronized (this) {

      // 1. Delete pathsToDelete
      for (Path meta : pathsToDelete) {
        LOG.debug("move: deleting metadata {}", meta);
        delete(meta);
      }

      // 2. Create new destination path metadata
      for (PathMetadata meta : pathsToCreate) {
        LOG.debug("move: adding metadata {}", meta);
        put(meta);
      }

      // 3. We now know full contents of all dirs in destination subtree
      for (PathMetadata meta : pathsToCreate) {
        FileStatus status = meta.getFileStatus();
        if (status == null || status.isDirectory()) {
          continue;
        }
        DirListingMetadata dir = listChildren(status.getPath());
        if (dir != null) {  // could be evicted already
          dir.setAuthoritative(true);
        }
      }
    }
  }

  @Override
  public void put(PathMetadata meta) throws IOException {

    Preconditions.checkNotNull(meta);
    FileStatus status = meta.getFileStatus();
    Path path = standardize(status.getPath());
    synchronized (this) {

      /* Add entry for this file. */
      if (LOG.isDebugEnabled()) {
        LOG.debug("put {} -> {}", path, meta.prettyPrint());
      }
      fileHash.put(path, meta);

      /* Directory case:
       * We also make sure we have an entry in the dirHash, so subsequent
       * listStatus(path) at least see the directory.
       *
       * If we had a boolean flag argument "isNew", we would know whether this
       * is an existing directory the client discovered via getFileStatus(),
       * or if it is a newly-created directory.  In the latter case, we would
       * be able to mark the directory as authoritative (fully-cached),
       * saving round trips to underlying store for subsequent listStatus()
       */

      if (status.isDirectory()) {
        DirListingMetadata dir = dirHash.mruGet(path);
        if (dir == null) {
          dirHash.put(path, new DirListingMetadata(path, DirListingMetadata
              .EMPTY_DIR, false));
        }
      }

      /* Update cached parent dir. */
      Path parentPath = path.getParent();
      if (parentPath != null) {
        DirListingMetadata parent = dirHash.mruGet(parentPath);
        if (parent == null) {
        /* Track this new file's listing in parent.  Parent is not
         * authoritative, since there may be other items in it we don't know
         * about. */
          parent = new DirListingMetadata(parentPath,
              DirListingMetadata.EMPTY_DIR, false);
          dirHash.put(parentPath, parent);
        }
        parent.put(status);
      }
    }
  }

  @Override
  public synchronized void put(DirListingMetadata meta) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("put dirMeta {}", meta.prettyPrint());
    }
    dirHash.put(standardize(meta.getPath()), meta);
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void destroy() throws IOException {
    if (dirHash != null) {
      dirHash.clear();
    }
  }

  @Override
  public synchronized void prune(long modTime) throws IOException {
    Iterator<Map.Entry<Path, PathMetadata>> files =
        fileHash.entrySet().iterator();
    while (files.hasNext()) {
      Map.Entry<Path, PathMetadata> entry = files.next();
      if (expired(entry.getValue().getFileStatus(), modTime)) {
        files.remove();
      }
    }
    Iterator<Map.Entry<Path, DirListingMetadata>> dirs =
        dirHash.entrySet().iterator();
    Collection<Path> ancestors = new LinkedList<>();
    while (dirs.hasNext()) {
      Map.Entry<Path, DirListingMetadata> entry = dirs.next();
      Path path = entry.getKey();
      DirListingMetadata metadata = entry.getValue();
      Collection<PathMetadata> oldChildren = metadata.getListing();
      Collection<PathMetadata> newChildren = new LinkedList<>();

      for (PathMetadata child : oldChildren) {
        FileStatus status = child.getFileStatus();
        if (!expired(status, modTime)) {
          newChildren.add(child);
        }
      }
      if (newChildren.size() == 0) {
        dirs.remove();
        ancestors.add(entry.getKey());
      } else {
        dirHash.put(path, new DirListingMetadata(path, newChildren, false));
      }
    }
  }

  private boolean expired(FileStatus status, long expiry) {
    // Note: S3 doesn't track modification time on directories, so for
    // consistency with the DynamoDB implementation we ignore that here
    return status.getModificationTime() < expiry && !status.isDirectory();
  }

  @VisibleForTesting
  static <T> void deleteHashByAncestor(Path ancestor, Map<Path, T> hash,
                                       boolean tombstone) {
    for (Iterator<Map.Entry<Path, T>> it = hash.entrySet().iterator();
         it.hasNext();) {
      Map.Entry<Path, T> entry = it.next();
      Path f = entry.getKey();
      T meta = entry.getValue();
      if (isAncestorOf(ancestor, f)) {
        if (tombstone) {
          if (meta instanceof PathMetadata) {
            entry.setValue((T) PathMetadata.tombstone(f));
          } else if (meta instanceof DirListingMetadata) {
            it.remove();
          } else {
            throw new IllegalStateException("Unknown type in hash");
          }
        } else {
          it.remove();
        }
      }
    }
  }

  /**
   * @return true iff 'ancestor' is ancestor dir in path 'f'.
   * All paths here are absolute.  Dir does not count as its own ancestor.
   */
  private static boolean isAncestorOf(Path ancestor, Path f) {
    String aStr = ancestor.toString();
    if (!ancestor.isRoot()) {
      aStr += "/";
    }
    String fStr = f.toString();
    return (fStr.startsWith(aStr));
  }

  /**
   * Update fileHash and dirHash to reflect deletion of file 'f'.  Call with
   * lock held.
   */
  private void deleteHashEntries(Path path, boolean tombstone) {

    // Remove target file/dir
    LOG.debug("delete file entry for {}", path);
    if (tombstone) {
      fileHash.put(path, PathMetadata.tombstone(path));
    } else {
      fileHash.remove(path);
    }

    // Update this and parent dir listing, if any

    /* If this path is a dir, remove its listing */
    LOG.debug("removing listing of {}", path);

    dirHash.remove(path);

    /* Remove this path from parent's dir listing */
    Path parent = path.getParent();
    if (parent != null) {
      DirListingMetadata dir = dirHash.get(parent);
      if (dir != null) {
        LOG.debug("removing parent's entry for {} ", path);
        if (tombstone) {
          dir.markDeleted(path);
        } else {
          dir.remove(path);
        }
      }
    }
  }

  /**
   * Return a "standardized" version of a path so we always have a consistent
   * hash value.  Also asserts the path is absolute, and contains host
   * component.
   * @param p input Path
   * @return standardized version of Path, suitable for hash key
   */
  private Path standardize(Path p) {
    Preconditions.checkArgument(p.isAbsolute(), "Path must be absolute");
    URI uri = p.toUri();
    if (uriHost != null) {
      Preconditions.checkArgument(!isEmpty(uri.getHost()));
    }
    return p;
  }

  private static boolean isEmpty(String s) {
    return (s == null || s.isEmpty());
  }
}
