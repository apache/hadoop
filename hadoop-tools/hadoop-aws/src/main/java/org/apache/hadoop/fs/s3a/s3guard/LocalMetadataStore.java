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

import javax.annotation.Nullable;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

  private String username;

  private ITtlTimeProvider ttlTimeProvider;

  @Override
  public void initialize(FileSystem fileSystem,
      ITtlTimeProvider ttlTp) throws IOException {
    Preconditions.checkNotNull(fileSystem);
    fs = fileSystem;
    URI fsURI = fs.getUri();
    uriHost = fsURI.getHost();
    if (uriHost != null && uriHost.equals("")) {
      uriHost = null;
    }

    initialize(fs.getConf(), ttlTp);
  }

  @Override
  public void initialize(Configuration conf, ITtlTimeProvider ttlTp)
      throws IOException {
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
    username = UserGroupInformation.getCurrentUser().getShortUserName();
    this.ttlTimeProvider = ttlTp;
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
  public void delete(Path p,
      final BulkOperationState operationState)
      throws IOException {
    doDelete(p, false, true);
  }

  @Override
  public void forgetMetadata(Path p) throws IOException {
    doDelete(p, false, false);
  }

  @Override
  public void deleteSubtree(Path path,
      final BulkOperationState operationState)
      throws IOException {
    doDelete(path, true, true);
  }

  private synchronized void doDelete(Path p, boolean recursive,
      boolean tombstone) {

    Path path = standardize(p);

    // Delete entry from file cache, then from cached parent directory, if any
    deleteCacheEntries(path, tombstone);

    if (recursive) {
      // Remove all entries that have this dir as path prefix.
      deleteEntryByAncestor(path, localCache, tombstone, ttlTimeProvider);
    }
  }

  @Override
  public void deletePaths(final Collection<Path> paths,
      @Nullable final BulkOperationState operationState) throws IOException {
    for (Path path : paths) {
      doDelete(path, false, true);
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

    if (listing != null) {
      // Make a copy so callers can mutate without affecting our state
      return new DirListingMetadata(listing);
    }
    return null;
  }

  @Override
  public void move(@Nullable Collection<Path> pathsToDelete,
      @Nullable Collection<PathMetadata> pathsToCreate,
      @Nullable final BulkOperationState operationState) throws IOException {
    LOG.info("Move {} to {}", pathsToDelete, pathsToCreate);

    if (pathsToCreate == null) {
      pathsToCreate = Collections.emptyList();
    }
    if (pathsToDelete == null) {
      pathsToDelete = Collections.emptyList();
    }

    // I feel dirty for using reentrant lock. :-|
    synchronized (this) {

      // 1. Delete pathsToDelete
      for (Path meta : pathsToDelete) {
        LOG.debug("move: deleting metadata {}", meta);
        delete(meta, null);
      }

      // 2. Create new destination path metadata
      for (PathMetadata meta : pathsToCreate) {
        LOG.debug("move: adding metadata {}", meta);
        put(meta, null);
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
  public void put(final PathMetadata meta) throws IOException {
    put(meta, null);
  }

  @Override
  public void put(PathMetadata meta,
      final BulkOperationState operationState) throws IOException {

    Preconditions.checkNotNull(meta);
    S3AFileStatus status = meta.getFileStatus();
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
          parentDirMeta.setLastUpdated(meta.getLastUpdated());
          parentMeta.setDirListingMetadata(parentDirMeta);
        }

        // Add the child pathMetadata to the listing
        parentMeta.getDirListingMeta().put(meta);

        // Mark the listing entry as deleted if the meta is set to deleted
        if(meta.isDeleted()) {
          parentMeta.getDirListingMeta().markDeleted(path,
              ttlTimeProvider.getNow());
        }
      }
    }
  }

  @Override
  public synchronized void put(DirListingMetadata meta,
      final List<Path> unchangedEntries,
      final BulkOperationState operationState) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("put dirMeta {}", meta.prettyPrint());
    }
    LocalMetadataEntry entry =
        localCache.getIfPresent(standardize(meta.getPath()));
    if (entry == null) {
      localCache.put(standardize(meta.getPath()), new LocalMetadataEntry(meta));
    } else {
      entry.setDirListingMetadata(meta);
    }
    put(meta.getListing(), null);
  }

  public synchronized void put(Collection<? extends PathMetadata> metas,
      final BulkOperationState operationState) throws
      IOException {
    for (PathMetadata meta : metas) {
      put(meta, operationState);
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
  public void prune(PruneMode pruneMode, long cutoff) throws IOException{
    prune(pruneMode, cutoff, "");
  }

  @Override
  public synchronized long prune(PruneMode pruneMode, long cutoff,
      String keyPrefix) {
    // prune files
    AtomicLong count = new AtomicLong();
    // filter path_metadata (files), filter expired, remove expired
    localCache.asMap().entrySet().stream()
        .filter(entry -> entry.getValue().hasPathMeta())
        .filter(entry -> expired(pruneMode,
            entry.getValue().getFileMeta(), cutoff, keyPrefix))
        .forEach(entry -> {
          localCache.invalidate(entry.getKey());
          count.incrementAndGet();
        });


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
            if (!expired(pruneMode, child, cutoff, keyPrefix)) {
              newChildren.add(child);
            } else {
              count.incrementAndGet();
            }
          }
          removeAuthoritativeFromParent(path, oldChildren, newChildren);
        });
    return count.get();
  }

  private void removeAuthoritativeFromParent(Path path,
      Collection<PathMetadata> oldChildren,
      Collection<PathMetadata> newChildren) {
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
  }

  private boolean expired(PruneMode pruneMode, PathMetadata metadata,
      long cutoff, String keyPrefix) {
    final S3AFileStatus status = metadata.getFileStatus();
    final URI statusUri = status.getPath().toUri();

    // remove the protocol from path string to be able to compare
    String bucket = statusUri.getHost();
    String statusTranslatedPath = "";
    if(bucket != null && !bucket.isEmpty()){
      // if there's a bucket, (well defined host in Uri) the pathToParentKey
      // can be used to get the path from the status
      statusTranslatedPath =
          PathMetadataDynamoDBTranslation.pathToParentKey(status.getPath());
    } else {
      // if there's no bucket in the path the pathToParentKey will fail, so
      // this is the fallback to get the path from status
      statusTranslatedPath = statusUri.getPath();
    }

    boolean expired;
    switch (pruneMode) {
    case ALL_BY_MODTIME:
      // Note: S3 doesn't track modification time on directories, so for
      // consistency with the DynamoDB implementation we ignore that here
      expired = status.getModificationTime() < cutoff && !status.isDirectory()
          && statusTranslatedPath.startsWith(keyPrefix);
      break;
    case TOMBSTONES_BY_LASTUPDATED:
      expired = metadata.getLastUpdated() < cutoff && metadata.isDeleted()
          && statusTranslatedPath.startsWith(keyPrefix);
      break;
    default:
      throw new UnsupportedOperationException("Unsupported prune mode: "
          + pruneMode);
    }

    return expired;
  }

  @VisibleForTesting
  static void deleteEntryByAncestor(Path ancestor,
      Cache<Path, LocalMetadataEntry> cache, boolean tombstone,
      ITtlTimeProvider ttlTimeProvider) {

    cache.asMap().entrySet().stream()
        .filter(entry -> isAncestorOf(ancestor, entry.getKey()))
        .forEach(entry -> {
          LocalMetadataEntry meta = entry.getValue();
          Path path = entry.getKey();
          if(meta.hasDirMeta()){
            cache.invalidate(path);
          } else if(tombstone && meta.hasPathMeta()){
            final PathMetadata pmTombstone = PathMetadata.tombstone(path,
                ttlTimeProvider.getNow());
            meta.setPathMetadata(pmTombstone);
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
        PathMetadata pmd = PathMetadata.tombstone(path,
            ttlTimeProvider.getNow());
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
          dir.markDeleted(path, ttlTimeProvider.getNow());
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

  @Override
  public RenameTracker initiateRenameOperation(final StoreContext storeContext,
      final Path source,
      final S3AFileStatus sourceStatus, final Path dest) throws IOException {
    return new ProgressiveRenameTracker(storeContext, this, source, dest,
        null);
  }

  @Override
  public synchronized void setTtlTimeProvider(ITtlTimeProvider ttlTimeProvider) {
    this.ttlTimeProvider = ttlTimeProvider;
  }

  @Override
  public synchronized void addAncestors(final Path qualifiedPath,
      @Nullable final BulkOperationState operationState) throws IOException {

    Collection<PathMetadata> newDirs = new ArrayList<>();
    Path parent = qualifiedPath.getParent();
    while (!parent.isRoot()) {
      PathMetadata directory = get(parent);
      if (directory == null || directory.isDeleted()) {
        S3AFileStatus status = new S3AFileStatus(Tristate.FALSE, parent,
            username);
        PathMetadata meta = new PathMetadata(status, Tristate.FALSE, false,
            ttlTimeProvider.getNow());
        newDirs.add(meta);
      } else {
        break;
      }
      parent = parent.getParent();
    }
    if (!newDirs.isEmpty()) {
      put(newDirs, operationState);
    }
  }
}
