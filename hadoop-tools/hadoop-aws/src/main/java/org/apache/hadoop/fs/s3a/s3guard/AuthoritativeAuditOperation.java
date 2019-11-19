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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.impl.AbstractStoreOperation;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;

/**
 * Audit a directory tree for being authoritative.
 * One aspect of the audit to be aware of: the root directory is
 * always considered authoritative, even though, because there is no
 * matching entry in any of the stores, it is not strictly true.
 */
public class AuthoritativeAuditOperation extends AbstractStoreOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      AuthoritativeAuditOperation.class);

  /**
   * Exception error code when a path is nonauth in the DB: {@value}.
   */
  public static final int ERROR_ENTRY_NOT_AUTH_IN_DDB = 4;

  /**
   * Exception error code when a path is not configured to be
   * auth in the S3A FS Config: {@value}.
   */
  public static final int ERROR_PATH_NOT_AUTH_IN_FS = 5;

  /**
   * Exception error string: {@value}.
   */
  public static final String E_NONAUTH
      = "Directory is not marked as authoritative in the S3Guard store";

  /** The metastore to audit. */
  private final DynamoDBMetadataStore metastore;

  /**  require all directories to be authoritative. */
  private final boolean requireAuthoritative;

  /**
   * Constructor.
   * @param storeContext store context.
   * @param metastore metastore
   * @param requireAuthoritative require all directories to be authoritative
   */
  public AuthoritativeAuditOperation(
      final StoreContext storeContext,
      final DynamoDBMetadataStore metastore,
      final boolean requireAuthoritative) {
    super(storeContext);
    this.metastore = metastore;
    this.requireAuthoritative = requireAuthoritative;
  }

  /**
   * Examine the path metadata and verify that the dir is authoritative.
   * @param md metadata.
   * @param requireAuth require all directories to be authoritative
   * @throws NonAuthoritativeDirException if it is non-auth and requireAuth=true.
   */
  private void verifyAuthDir(final DDBPathMetadata md,
      final boolean requireAuth)
      throws PathIOException {
    final Path path = md.getFileStatus().getPath();
    boolean isAuth = path.isRoot() || md.isAuthoritativeDir();
    if (!isAuth && requireAuth) {
      throw new NonAuthoritativeDirException(path);
    }
  }

  /**
   * Examine the path metadata, declare whether it should be queued for
   * recursive scanning.
   * @param md metadata.
   * @return true if it is a dir to scan.
   */
  private boolean isDirectory(PathMetadata md) {
    return !md.getFileStatus().isFile();
  }

  /**
   * Audit the tree.
   * @param path qualified path to scan
   * @return tuple(dirs scanned, nonauth dirs found)
   * @throws IOException IO failure
   * @throws ExitUtil.ExitException if a non-auth dir was found.
   */
  public Pair<Integer, Integer> audit(Path path) throws IOException {
    try (DurationInfo ignored =
             new DurationInfo(LOG, "audit %s", path)) {
      return executeAudit(path, requireAuthoritative, true);
    } catch (NonAuthoritativeDirException p) {
      throw new ExitUtil.ExitException(
          ERROR_ENTRY_NOT_AUTH_IN_DDB,
          p.toString(),
          p);
    }
  }

  /**
   * Audit the tree.
   * This is the internal code which throws a NonAuthoritativePathException
   * on failures; tests may use it.
   * @param path path to scan
   * @param requireAuth require all directories to be authoritative
   * @param recursive recurse?
   * @return tuple(dirs scanned, nonauth dirs found)
   * @throws IOException IO failure
   * @throws NonAuthoritativeDirException if a non-auth dir was found.
   */
  @VisibleForTesting
  Pair<Integer, Integer> executeAudit(final Path path,
      final boolean requireAuth,
      final boolean recursive) throws IOException {
    int dirs = 0;
    int nonauth = 0;
    final Queue<DDBPathMetadata> queue = new ArrayDeque<>();
    final boolean isRoot = path.isRoot();
    final DDBPathMetadata baseData = metastore.get(path);
    if (baseData == null) {
      throw new ExitUtil.ExitException(LauncherExitCodes.EXIT_NOT_FOUND,
          "No S3Guard entry for path " + path);
    }

    if (isRoot || isDirectory(baseData)) {
      // we have the root entry or an authoritative a directory
      queue.add(baseData);
    } else {
      LOG.info("Path represents file");
      return Pair.of(0, 0);
    }

    while (!queue.isEmpty()) {
      dirs++;
      final DDBPathMetadata dir = queue.poll();
      LOG.info("Directory {} {} authoritative",
          dir.getFileStatus().getPath(),
          dir.isAuthoritativeDir() ? "is" : "is not");
      LOG.debug("Directory {}", dir);
      verifyAuthDir(dir, requireAuth);

      // list its children
      if (recursive) {
        final DirListingMetadata entry = metastore.listChildren(
            dir.getFileStatus().getPath());

        if (entry != null) {
          final Collection<PathMetadata> listing = entry.getListing();
          int files = 0, subdirs = 0;
          for (PathMetadata e : listing) {
            if (isDirectory(e)) {
              final DDBPathMetadata e1 = (DDBPathMetadata) e;
              verifyAuthDir(e1, requireAuth);
              queue.add(e1);
              subdirs++;
            } else {
              files++;
            }
          }
          LOG.info("  files {}; directories {}", files, subdirs);
        } else {
          LOG.info("Directory {} has been deleted", dir);
        }
      }
    }
    return Pair.of(dirs, nonauth);
  }

  /**
   * A directory was found which was non-authoritative.
   */
  public static class NonAuthoritativeDirException extends PathIOException {

    public NonAuthoritativeDirException(final Path path) {
      super(path.toString(), E_NONAUTH);
    }
  }

}
