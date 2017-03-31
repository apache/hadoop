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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Logic for integrating MetadataStore with S3A.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class S3Guard {
  private static final Logger LOG = LoggerFactory.getLogger(S3Guard.class);

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  public static final String S3GUARD_DDB_CLIENT_FACTORY_IMPL =
      "fs.s3a.s3guard.ddb.client.factory.impl";

  static final Class<? extends DynamoDBClientFactory>
      S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT =
      DynamoDBClientFactory.DefaultDynamoDBClientFactory.class;

  // Utility class.  All static functions.
  private S3Guard() { }

  /* Utility functions. */

  /**
   * Create a new instance of the configured MetadataStore.
   * The returned MetadataStore will have been initialized via
   * {@link MetadataStore#initialize(FileSystem)} by this function before
   * returning it.  Callers must clean up by calling
   * {@link MetadataStore#close()} when done using the MetadataStore.
   *
   * If this fails to instantiate the class specified in config (fs.s3a
   * .metadatastore.impl), or there is an error initializing it, this will log
   * an error and return an instance of {@link NullMetadataStore} instead.
   *
   * @param fs  FileSystem whose Configuration specifies which
   *            implementation to use.
   * @return Reference to new MetadataStore.
   * @throws IOException if the metadata store cannot be instantiated
   */
  public static MetadataStore getMetadataStore(FileSystem fs)
      throws IOException {
    Preconditions.checkNotNull(fs);
    Configuration conf = fs.getConf();
    Preconditions.checkNotNull(conf);
    MetadataStore msInstance;
    try {
      Class<? extends MetadataStore> msClass = getMetadataStoreClass(conf);
      msInstance = ReflectionUtils.newInstance(msClass, conf);
      LOG.debug("Using {} metadata store for {} filesystem",
          msClass.getSimpleName(), fs.getScheme());
      msInstance.initialize(fs);
      return msInstance;
    } catch (RuntimeException | IOException e) {
      String message = "Failed to instantiate metadata store " +
          conf.get(S3_METADATA_STORE_IMPL)
          + " defined in " + S3_METADATA_STORE_IMPL
          + ": " + e;
      LOG.error(message, e);
      if (e instanceof IOException) {
        throw e;
      } else {
        throw new IOException(message, e);
      }
    }
  }

  private static Class<? extends MetadataStore> getMetadataStoreClass(
      Configuration conf) {
    if (conf == null) {
      return NullMetadataStore.class;
    }

    return conf.getClass(S3_METADATA_STORE_IMPL, NullMetadataStore.class,
            MetadataStore.class);
  }


  /**
   * Helper function which puts given S3AFileStatus in the MetadataStore and
   * returns the same S3AFileStatus.
   * @param ms MetadataStore to put() into.
   * @param status status to store
   * @return The same status as passed in
   * @throws IOException if metadata store update failed
   */
  public static S3AFileStatus putAndReturn(MetadataStore ms,
      S3AFileStatus status) throws IOException {

    ms.put(new PathMetadata(status));
    return status;
  }

  public static FileStatus[] dirMetaToStatuses(DirListingMetadata dirMeta)  {
    Collection<PathMetadata> listing = dirMeta.getListing();
    FileStatus[] statuses = new FileStatus[listing.size()];

    // HADOOP-13760: filter out deleted entries here
    int i = 0;
    for (PathMetadata pm : listing) {
      statuses[i++] = pm.getFileStatus();
    }

    return statuses;
  }

  /**
   * Given directory listing metadata from both the backing store, and the
   * MetadataStore, merge the two sources of truth to create a consistent
   * view of the current directory contents, which can be returned to clients.
   *
   * Also update the MetadataStore to reflect the resulting directory listing.
   *
   * @param ms MetadataStore to use.
   * @param path path to directory
   * @param backingStatuses Directory listing from the backing store.
   * @param dirMeta  Directory listing from MetadataStore.  May be null.
   * @param isAuthoritative State of authoritative mode
   * @return Final result of directory listing.
   * @throws IOException if metadata store update failed
   */
  public static FileStatus[] dirListingUnion(MetadataStore ms, Path path,
      List<FileStatus> backingStatuses, DirListingMetadata dirMeta,
      boolean isAuthoritative) throws IOException {

    // Fast-path for NullMetadataStore
    if (ms instanceof NullMetadataStore) {
      return backingStatuses.toArray(new FileStatus[backingStatuses.size()]);
    }

    assertQualified(path);

    if (dirMeta == null) {
      // The metadataStore had zero state for this directory
      dirMeta = new DirListingMetadata(path, DirListingMetadata.EMPTY_DIR,
          false);
    }

    // Since we treat the MetadataStore as a "fresher" or "consistent" view
    // of metadata, we always use its metadata first.

    // Since the authoritative case is already handled outside this function,
    // we will basically start with the set of directory entries in the
    // DirListingMetadata, and add any that only exist in the backingStatuses.

    // HADOOP-13760: filter out deleted files via PathMetadata#isDeleted() here

    boolean changed = false;
    for (FileStatus s : backingStatuses) {

      // Minor race condition here.  Multiple threads could add to this
      // mutable DirListingMetadata.  Since it is backed by a
      // ConcurrentHashMap, the last put() wins.  I think this is ok.
      // More concerning is two threads racing on listStatus() and delete().
      // Any FileSystem has similar race conditions, but we could persist
      // a stale entry longer.  We could expose an atomic
      // DirListingMetadata#putIfNotPresent()
      boolean updated = dirMeta.put(s);
      changed = changed || updated;
    }

    if (changed && isAuthoritative) {
      dirMeta.setAuthoritative(true); // This is the full directory contents
      ms.put(dirMeta);
    }

    return dirMetaToStatuses(dirMeta);
  }

  /**
   * Although NullMetadataStore does nothing, callers may wish to avoid work
   * (fast path) when the NullMetadataStore is in use.
   * @param ms The MetadataStore to test
   * @return true iff the MetadataStore is the null, or no-op, implementation.
   */
  public static boolean isNullMetadataStore(MetadataStore ms) {
    return (ms instanceof NullMetadataStore);
  }

  /**
   * Update MetadataStore to reflect creation of the given  directories.
   * @param ms    MetadataStore to update.
   * @param dirs  null, or an ordered list of directories from leaf to root.
   *              E.g. if /a/ exists, and  mkdirs(/a/b/c/d) is called, this
   *              list will contain [/a/b/c/d, /a/b/c, /a/b].   /a/b/c/d is
   *              an empty, dir, and the other dirs only contain their child
   *              dir.
   * @param owner Hadoop user name
   */
  public static void makeDirsOrdered(MetadataStore ms, List<Path> dirs,
      String owner) {
    if (dirs == null) {
      return;
    }

    /* We discussed atomicity of this implementation.
     * The concern is that multiple clients could race to write different
     * cached directories to the MetadataStore.  Two solutions are proposed:
     * 1. Move mkdirs() into MetadataStore interface and let implementations
     *    ensure they are atomic.
     * 2. Specify that the semantics of MetadataStore#putListStatus() is
     *    always additive,  That is, if MetadataStore has listStatus() state
     *    for /a/b that contains [/a/b/file0, /a/b/file1], and we then call
     *    putListStatus(/a/b -> [/a/b/file2, /a/b/file3], isAuthoritative=true),
     *    then we will end up with final state of
     *    [/a/b/file0, /a/b/file1, /a/b/file2, /a/b/file3], isAuthoritative =
     *    true
     */
    FileStatus prevStatus = null;
    // Iterate from leaf to root
    for (int i = 0; i < dirs.size(); i++) {
      boolean isLeaf = (prevStatus == null);
      Path f = dirs.get(i);
      assertQualified(f);
      FileStatus status = S3AUtils.createUploadFileStatus(f, true, 0, 0, owner);
      Collection<PathMetadata> children;
      if (isLeaf) {
        children = DirListingMetadata.EMPTY_DIR;
      } else {
        children = new ArrayList<>(1);
        children.add(new PathMetadata(prevStatus));
      }
      DirListingMetadata dirMeta = new DirListingMetadata(f, children, true);
      try {
        ms.put(dirMeta);
        ms.put(new PathMetadata(status));
      } catch (IOException ioe) {
        LOG.error("MetadataStore#put() failure:", ioe);
        return;
      }
      prevStatus = status;
    }
  }

  /**
   * Helper function that records the move of directory path, adding
   * resulting metadata to the supplied lists.  Does not store in MetadataStore.
   * @param ms  MetadataStore, used to make this a no-op, when it is
   *            NullMetadataStore.
   * @param srcPaths stores the source path here
   * @param dstMetas stores destination metadata here
   * @param srcPath  source path to store
   * @param dstPath  destination path to store
   * @param owner Hadoop user name
   */
  public static void addMoveDir(MetadataStore ms, Collection<Path> srcPaths,
      Collection<PathMetadata> dstMetas, Path srcPath, Path dstPath,
      String owner) {
    if (isNullMetadataStore(ms)) {
      return;
    }
    assertQualified(srcPath);
    assertQualified(dstPath);
    FileStatus dstStatus = S3AUtils.createUploadFileStatus(dstPath, true, 0,
        0, owner);
    addMoveStatus(srcPaths, dstMetas, srcPath, dstStatus);
  }

  /**
   * Like {@link #addMoveDir(MetadataStore, Collection, Collection, Path,
   * Path, String)} (), but for files.
   * @param ms  MetadataStore, used to make this a no-op, when it is
   *            NullMetadataStore.
   * @param srcPaths stores the source path here
   * @param dstMetas stores destination metadata here
   * @param srcPath  source path to store
   * @param dstPath  destination path to store
   * @param size length of file moved
   * @param blockSize  blocksize to associate with destination file
   * @param owner Hadoop user name
   */
  public static void addMoveFile(MetadataStore ms, Collection<Path> srcPaths,
      Collection<PathMetadata> dstMetas, Path srcPath, Path dstPath,
      long size, long blockSize, String owner) {
    if (isNullMetadataStore(ms)) {
      return;
    }
    assertQualified(srcPath);
    assertQualified(dstPath);
    FileStatus dstStatus = S3AUtils.createUploadFileStatus(dstPath, false,
        size, blockSize, owner);
    addMoveStatus(srcPaths, dstMetas, srcPath, dstStatus);
  }

  /**
   * Helper method that records the move of all ancestors of a path.
   *
   * In S3A, an optimization is to delete unnecessary fake directory objects if
   * the directory is non-empty. In that case, for a nested child to move, S3A
   * is not listing and thus moving all its ancestors (up to source root). So we
   * take care of those inferred directories of this path explicitly.
   *
   * As {@link #addMoveFile} and {@link #addMoveDir}, this method adds resulting
   * metadata to the supplied lists. It does not store in MetadataStore.
   *
   * @param ms MetadataStore, no-op if it is NullMetadataStore
   * @param srcPaths stores the source path here
   * @param dstMetas stores destination metadata here
   * @param srcRoot source root up to which (exclusive) should we add ancestors
   * @param srcPath source path of the child to add ancestors
   * @param dstPath destination path of the child to add ancestors
   * @param owner Hadoop user name
   */
  public static void addMoveAncestors(MetadataStore ms,
      Collection<Path> srcPaths, Collection<PathMetadata> dstMetas,
      Path srcRoot, Path srcPath, Path dstPath, String owner) {
    if (isNullMetadataStore(ms)) {
      return;
    }

    assertQualified(srcRoot);
    assertQualified(srcPath);
    assertQualified(dstPath);

    if (srcPath.equals(srcRoot)) {
      LOG.debug("Skip moving ancestors of source root directory {}", srcRoot);
      return;
    }

    Path parentSrc = srcPath.getParent();
    Path parentDst = dstPath.getParent();
    while (parentSrc != null
        && !parentSrc.isRoot()
        && !parentSrc.equals(srcRoot)
        && !srcPaths.contains(parentSrc)) {
      LOG.debug("Renaming non-listed parent {} to {}", parentSrc, parentDst);
      S3Guard.addMoveDir(ms, srcPaths, dstMetas, parentSrc, parentDst, owner);
      parentSrc = parentSrc.getParent();
      parentDst = parentDst.getParent();
    }
  }

  private static void addMoveStatus(Collection<Path> srcPaths,
      Collection<PathMetadata> dstMetas, Path srcPath, FileStatus dstStatus)
  {
    srcPaths.add(srcPath);
    dstMetas.add(new PathMetadata(dstStatus));
  }

  public static void assertQualified(Path p) {
    URI uri = p.toUri();
    // Paths must include bucket in case MetadataStore is shared between
    // multiple S3AFileSystem instances
    Preconditions.checkNotNull(uri.getHost(), "Null host in " + uri);

    // I believe this should never fail, since there is a host?
    Preconditions.checkNotNull(uri.getScheme(), "Null scheme in " + uri);
  }
}
