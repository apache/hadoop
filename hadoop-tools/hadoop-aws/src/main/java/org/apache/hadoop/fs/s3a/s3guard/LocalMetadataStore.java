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
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
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
  private boolean isS3A;
  /* Null iff this FS does not have an associated URI host. */
  private String uriHost;

  @Override
  public void initialize(FileSystem fileSystem) throws IOException {
    Preconditions.checkNotNull(fileSystem);
    fs = fileSystem;

    // Hate to take a dependency on S3A, but if the MetadataStore has to
    // maintain S3AFileStatus#isEmptyDirectory, best to be able to to that
    // under our own lock.
    if (fs instanceof S3AFileSystem) {
      isS3A = true;
    }

    URI fsURI = fs.getUri();
    uriHost = fsURI.getHost();
    if (uriHost != null && uriHost.equals("")) {
      uriHost = null;
    }

    Configuration conf = fs.getConf();
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
  public void delete(Path path) throws IOException {
    doDelete(path, false);
  }

  @Override
  public void deleteSubtree(Path path) throws IOException {
    doDelete(path, true);
  }

  private synchronized void doDelete(Path p, boolean recursive) {

    Path path = standardize(p);
    // We could implement positive hit for 'deleted' files.  For now we
    // do not track them.

    // Delete entry from file cache, then from cached parent directory, if any

    removeHashEntries(path);

    if (recursive) {
      // Remove all entries that have this dir as path prefix.
      clearHashByAncestor(path, dirHash);
      clearHashByAncestor(path, fileHash);
    }
  }

  @Override
  public synchronized PathMetadata get(Path p) throws IOException {
    Path path = standardize(p);
    PathMetadata m = fileHash.mruGet(path);
    LOG.debug("get({}) -> {}", path, m == null ? "null" : m.prettyPrint());
    return m;
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
    return listing;
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
      for (Path p : pathsToDelete) {
        LOG.debug("move: deleting metadata {}", p);
        delete(p);
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
        } else {
          // S3A-specific logic to maintain S3AFileStatus#isEmptyDirectory()
          if (isS3A) {
            setS3AIsEmpty(parentPath, false);
          }
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

  @VisibleForTesting
  static <T> void clearHashByAncestor(Path ancestor, Map<Path, T> hash) {
    for (Iterator<Map.Entry<Path, T>> it = hash.entrySet().iterator();
         it.hasNext();) {
      Map.Entry<Path, T> entry = it.next();
      Path f = entry.getKey();
      if (isAncestorOf(ancestor, f)) {
        it.remove();
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
  private void removeHashEntries(Path path) {

    // Remove target file/dir
    LOG.debug("delete file entry for {}", path);
    fileHash.remove(path);

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
        dir.remove(path);

        // S3A-specific logic dealing with S3AFileStatus#isEmptyDirectory()
        if (isS3A) {
          if (dir.isAuthoritative() && dir.numEntries() == 0) {
            setS3AIsEmpty(parent, true);
          } else if (dir.numEntries() == 0) {
            // We do not know of any remaining entries in parent directory.
            // However, we do not have authoritative listing, so there may
            // still be some entries in the dir.  Since we cannot know the
            // proper state of the parent S3AFileStatus#isEmptyDirectory, we
            // will invalidate our entries for it.
            // Better than deleting entries would be marking them as "missing
            // metadata".  Deleting them means we lose consistent listing and
            // ability to retry for eventual consistency for the parent path.

            // TODO implement missing metadata feature
            invalidateFileStatus(parent);
          }
          // else parent directory still has entries in it, isEmptyDirectory
          // does not change
        }

      }
    }
  }

  /**
   * Invalidate any stored FileStatus's for path.  Call with lock held.
   * @param path path to invalidate
   */
  private void invalidateFileStatus(Path path) {
    // TODO implement missing metadata feature
    fileHash.remove(path);
    Path parent = path.getParent();
    if (parent != null) {
      DirListingMetadata parentListing = dirHash.get(parent);
      if (parentListing != null) {
        parentListing.remove(path);
      }
    }
  }

  private void setS3AIsEmpty(Path path, boolean isEmpty) {
    // Update any file statuses in fileHash
    PathMetadata meta = fileHash.get(path);
    if (meta != null) {
      S3AFileStatus s3aStatus =  (S3AFileStatus)meta.getFileStatus();
      LOG.debug("Setting S3AFileStatus is empty dir ({}) for key {}, {}",
          isEmpty, path, s3aStatus.getPath());
      s3aStatus.setIsEmptyDirectory(isEmpty);
    }
    // Update any file statuses in dirHash
    Path parent = path.getParent();
    if (parent != null) {
      DirListingMetadata dirMeta = dirHash.get(parent);
      if (dirMeta != null) {
        PathMetadata entry = dirMeta.get(path);
        if (entry != null) {
          S3AFileStatus s3aStatus =  (S3AFileStatus)entry.getFileStatus();
          LOG.debug("Setting S3AFileStatus is empty dir ({}) for key " +
                  "{}, {}", isEmpty, path, s3aStatus.getPath());
          s3aStatus.setIsEmptyDirectory(isEmpty);
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
