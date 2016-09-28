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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  public static final int DEFAULT_MAX_RECORDS = 128;
  public static final String CONF_MAX_RECORDS =
      "fs.metadatastore.local.max_records";

  /** Contains directories and files. */
  private LruHashMap<Path, PathMetadata> fileHash;

  /** Contains directory listings. */
  private LruHashMap<Path, DirListingMetadata> dirHash;

  private FileSystem fs;

  @Override
  public void initialize(FileSystem fileSystem) throws IOException {
    Preconditions.checkNotNull(fileSystem);
    fs = fileSystem;
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

  private synchronized void doDelete(Path path, boolean recursive) {

    // We could implement positive hit for 'deleted' files.  For now we
    // do not track them.

    // Delete entry from file cache, then from cached parent directory, if any

    // Remove target file/dir
    fileHash.remove(path);

    // Update this and parent dir listing, if any
    dirHashDeleteFile(path);

    if (recursive) {
      // Remove all entries that have this dir as path prefix.
      clearHashByAncestor(path, dirHash);
      clearHashByAncestor(path, fileHash);
    }
  }

  @Override
  public synchronized PathMetadata get(Path path) throws IOException {
    return fileHash.mruGet(path);
  }

  @Override
  public synchronized DirListingMetadata listChildren(Path path) throws
      IOException {
    return dirHash.mruGet(path);
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
        delete(p);
      }

      // 2. Create new destination path metadata
      for (PathMetadata meta : pathsToCreate) {
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

    FileStatus status = meta.getFileStatus();
    Path path = status.getPath();
    synchronized (this) {

      /* Add entry for this file. */
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

      /* If directory, go ahead and cache the fact that it is empty. */
      if (status.isDirectory()) {
        DirListingMetadata dir = dirHash.mruGet(path);
        if (dir == null) {
          dirHash.put(path, new DirListingMetadata(path, DirListingMetadata
              .EMPTY_DIR, false));
        }
      }

      /* Update cached parent dir. */
      Path parentPath = path.getParent();
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

  @Override
  public synchronized void put(DirListingMetadata meta) throws IOException {
    dirHash.put(meta.getPath(), meta);
  }

  @Override
  public void close() throws IOException {

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
   * Update dirHash to reflect deletion of file 'f'.  Call with lock held.
   */
  private void dirHashDeleteFile(Path path) {

    /* If this path is a dir, remove its listing */
    dirHash.remove(path);

    /* Remove this path from parent's dir listing */
    Path parent = path.getParent();
    if (parent != null) {
      DirListingMetadata dir = dirHash.get(parent);
      if (dir != null) {
        dir.remove(path);
      }
    }
  }
}
