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

package org.apache.hadoop.fs.s3a.s3guard;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * This is a local, in-memory implementation of MetadataStore.
 * This is <i>not</i> a coherent cache across processes.  It is only
 * locally-coherent.
 *
 * The purpose of this is for unit and integration testing.
 * It could also be used to accelerate local-only operations where only one
 * process is operating on a given object store, or multiple processes are
 * accessing a read-only storage bucket.
 *
 * This MetadataStore does not enforce filesystem rules such as disallowing
 * non-recursive removal of non-empty directories.  It is assumed the caller
 * already has to perform these sorts of checks.
 *
 * Contains one cache internally with time based eviction.
 */
public class LocalMetadataStore implements MetadataStore {

  public static final Logger LOG = LoggerFactory.getLogger(MetadataStore.class);

  /** Contains directory and file listings. */
  private Cache<Path, LocalMetadataEntry> localCache;

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
    int maxRecords = conf.getInt(S3GUARD_METASTORE_LOCAL_MAX_RECORDS,
        DEFAULT_S3GUARD_METASTORE_LOCAL_MAX_RECORDS);
    if (maxRecords < 4) {
      maxRecords = 4;
    }
    int ttl = conf.getInt(S3GUARD_METASTORE_LOCAL_ENTRY_TTL,
        DEFAULT_S3GUARD_METASTORE_LOCAL_ENTRY_TTL);

    CacheBuilder builder = CacheBuilder.newBuilder().maximumSize(maxRecords);
    if (ttl >= 0) {
      builder.expireAfterAccess(ttl, TimeUnit.MILLISECONDS);
    }

    localCache = builder.build();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "LocalMetadataStore{");
    sb.append("uriHost='").append(uriHost).append('\'');
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

    deleteCacheEntries(path, tombstone);

    if (recursive) {
      // Remove all entries that have this dir as path prefix.
      deleteEntryByAncestor(path, localCache, tombstone);
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
      PathMetadata m = getFileMeta(path);

      if (wantEmptyDirectoryFlag && m != null &&
          m.getFileStatus().isDirectory()) {
        m.setIsEmptyDirectory(isEmptyDirectory(p));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("get({}) -> {}", path, m == null ? "null" : m.prettyPrint());
      }
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
    DirListingMetadata dlm = getDirListingMeta(p);
    return dlm.withoutTombstones().isEmpty();
  }

  @Override
  public synchronized DirListingMetadata listChildren(Path p) throws
      IOException {
    Path path = standardize(p);
    DirListingMetadata listing = getDirListingMeta(path);
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
    LOG.info("Move {} to {}", pathsToDelete, pathsToCreate);

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
      LocalMetadataEntry entry = localCache.getIfPresent(path);
      if(entry == null){
        entry = new LocalMetadataEntry(meta);
      } else {
        entry.setPathMetadata(meta);
      }

      /* Directory case:
       * We also make sure we have an entry in the dirCache, so subsequent
       * listStatus(path) at least see the directory.
       *
       * If we had a boolean flag argument "isNew", we would know whether this
       * is an existing directory the client discovered via getFileStatus(),
       * or if it is a newly-created directory.  In the latter case, we would
       * be able to mark the directory as authoritative (fully-cached),
       * saving round trips to underlying store for subsequent listStatus()
       */

      // only create DirListingMetadata if the entry does not have one
      if (status.isDirectory() && !entry.hasDirMeta()) {
        DirListingMetadata dlm =
            new DirListingMetadata(path, DirListingMetadata.EMPTY_DIR, false);
        entry.setDirListingMetadata(dlm);
      }
      localCache.put(path, entry);

      /* Update cached parent dir. */
      Path parentPath = path.getParent();
      if (parentPath != null) {
        LocalMetadataEntry parentMeta = localCache.getIfPresent(parentPath);

        // Create empty parent LocalMetadataEntry if it doesn't exist
        if (parentMeta == null){
          parentMeta = new LocalMetadataEntry();
          localCache.put(parentPath, parentMeta);
        }

        // If there is no directory metadata on the parent entry, create
        // an empty one
        if (!parentMeta.hasDirMeta()) {
          DirListingMetadata parentDirMeta =
              new DirListingMetadata(parentPath, DirListingMetadata.EMPTY_DIR,
                  false);
          parentMeta.setDirListingMetadata(parentDirMeta);
        }

        // Add the child status to the listing
        parentMeta.getDirListingMeta().put(status);

        // Mark the listing entry as deleted if the meta is set to deleted
        if(meta.isDeleted()) {
          parentMeta.getDirListingMeta().markDeleted(path);
        }
      }
    }
  }

  @Override
  public synchronized void put(DirListingMetadata meta) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("put dirMeta {}", meta.prettyPrint());
    }
    LocalMetadataEntry entry =
        localCache.getIfPresent(standardize(meta.getPath()));
    if(entry == null){
      localCache.put(standardize(meta.getPath()), new LocalMetadataEntry(meta));
    } else {
      entry.setDirListingMetadata(meta);
    }
    put(meta.getListing());
  }

  public synchronized void put(Collection<PathMetadata> metas) throws
      IOException {
    for (PathMetadata meta : metas) {
      put(meta);
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void destroy() throws IOException {
    if (localCache != null) {
      localCache.invalidateAll();
    }
  }

  @Override
  public void prune(long modTime) throws IOException{
    prune(modTime, "");
  }

  @Override
  public synchronized void prune(long modTime, String keyPrefix) {
    // prune files
    // filter path_metadata (files), filter expired, remove expired
    localCache.asMap().entrySet().stream()
        .filter(entry -> entry.getValue().hasPathMeta())
        .filter(entry -> expired(
            entry.getValue().getFileMeta().getFileStatus(), modTime, keyPrefix))
        .forEach(entry -> localCache.invalidate(entry.getKey()));


    // prune dirs
    // filter DIR_LISTING_METADATA, remove expired, remove authoritative bit
    localCache.asMap().entrySet().stream()
        .filter(entry -> entry.getValue().hasDirMeta())
        .forEach(entry -> {
          Path path = entry.getKey();
          DirListingMetadata metadata = entry.getValue().getDirListingMeta();
          Collection<PathMetadata> oldChildren = metadata.getListing();
          Collection<PathMetadata> newChildren = new LinkedList<>();

          for (PathMetadata child : oldChildren) {
            FileStatus status = child.getFileStatus();
            if (!expired(status, modTime, keyPrefix)) {
              newChildren.add(child);
            }
          }
          if (newChildren.size() != oldChildren.size()) {
            DirListingMetadata dlm =
                new DirListingMetadata(path, newChildren, false);
            localCache.put(path, new LocalMetadataEntry(dlm));
            if (!path.isRoot()) {
              DirListingMetadata parent = getDirListingMeta(path.getParent());
              if (parent != null) {
                parent.setAuthoritative(false);
              }
            }
          }
        });
  }

  private boolean expired(FileStatus status, long expiry, String keyPrefix) {
    // remove the protocol from path string to be able to compare
    String bucket = status.getPath().toUri().getHost();
    String statusTranslatedPath = "";
    if(bucket != null && !bucket.isEmpty()){
      // if there's a bucket, (well defined host in Uri) the pathToParentKey
      // can be used to get the path from the status
      statusTranslatedPath =
          PathMetadataDynamoDBTranslation.pathToParentKey(status.getPath());
    } else {
      // if there's no bucket in the path the pathToParentKey will fail, so
      // this is the fallback to get the path from status
      statusTranslatedPath = status.getPath().toUri().getPath();
    }

    // Note: S3 doesn't track modification time on directories, so for
    // consistency with the DynamoDB implementation we ignore that here
    return status.getModificationTime() < expiry && !status.isDirectory()
      && statusTranslatedPath.startsWith(keyPrefix);
  }

  @VisibleForTesting
  static void deleteEntryByAncestor(Path ancestor,
      Cache<Path, LocalMetadataEntry> cache, boolean tombstone) {

    cache.asMap().entrySet().stream()
        .filter(entry -> isAncestorOf(ancestor, entry.getKey()))
        .forEach(entry -> {
          LocalMetadataEntry meta = entry.getValue();
          Path path = entry.getKey();
          if(meta.hasDirMeta()){
            cache.invalidate(path);
          } else if(tombstone && meta.hasPathMeta()){
            meta.setPathMetadata(PathMetadata.tombstone(path));
          } else {
            cache.invalidate(path);
          }
        });
  }

  /**
   * @return true if 'ancestor' is ancestor dir in path 'f'.
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
   * Update fileCache and dirCache to reflect deletion of file 'f'.  Call with
   * lock held.
   */
  private void deleteCacheEntries(Path path, boolean tombstone) {
    LocalMetadataEntry entry = localCache.getIfPresent(path);
    // If there's no entry, delete should silently succeed
    // (based on MetadataStoreTestBase#testDeleteNonExisting)
    if(entry == null){
      LOG.warn("Delete: path {} is missing from cache.", path);
      return;
    }

    // Remove target file entry
    LOG.debug("delete file entry for {}", path);
    if(entry.hasPathMeta()){
      if (tombstone) {
        PathMetadata pmd = PathMetadata.tombstone(path);
        entry.setPathMetadata(pmd);
      } else {
        entry.setPathMetadata(null);
      }
    }

    // If this path is a dir, remove its listing
    if(entry.hasDirMeta()) {
      LOG.debug("removing listing of {}", path);
      entry.setDirListingMetadata(null);
    }

    // If the entry is empty (contains no dirMeta or pathMeta) remove it from
    // the cache.
    if(!entry.hasDirMeta() && !entry.hasPathMeta()){
      localCache.invalidate(entry);
    }

    /* Remove this path from parent's dir listing */
    Path parent = path.getParent();
    if (parent != null) {
      DirListingMetadata dir = getDirListingMeta(parent);
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
      Preconditions.checkArgument(StringUtils.isNotEmpty(uri.getHost()));
    }
    return p;
  }

  @Override
  public Map<String, String> getDiagnostics() throws IOException {
    Map<String, String> map = new HashMap<>();
    map.put("name", "local://metadata");
    map.put("uriHost", uriHost);
    map.put("description", "Local in-VM metadata store for testing");
    map.put(MetadataStoreCapabilities.PERSISTS_AUTHORITATIVE_BIT,
        Boolean.toString(true));
    return map;
  }

  @Override
  public void updateParameters(Map<String, String> parameters)
      throws IOException {
  }

  PathMetadata getFileMeta(Path p){
    LocalMetadataEntry entry = localCache.getIfPresent(p);
    if(entry != null && entry.hasPathMeta()){
      return entry.getFileMeta();
    } else {
      return null;
    }
  }

  DirListingMetadata getDirListingMeta(Path p){
    LocalMetadataEntry entry = localCache.getIfPresent(p);
    if(entry != null && entry.hasDirMeta()){
      return entry.getDirListingMeta();
    } else {
      return null;
    }
  }

}
