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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.impl.AbstractStoreOperation;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitCodeProvider;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;

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
   * Exception error code when a path is non-auth in the DB}.
   */
  public static final int ERROR_ENTRY_NOT_AUTH_IN_DDB = EXIT_NOT_ACCEPTABLE;

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
   * Verbose switch.
   */
  private final boolean verbose;

  /**
   * Constructor.
   * @param storeContext store context.
   * @param metastore metastore
   * @param requireAuthoritative require all directories to be authoritative
   * @param verbose verbose output
   */
  public AuthoritativeAuditOperation(
      final StoreContext storeContext,
      final DynamoDBMetadataStore metastore,
      final boolean requireAuthoritative,
      final boolean verbose) {
    super(storeContext);
    this.metastore = metastore;
    this.requireAuthoritative = requireAuthoritative;
    this.verbose = verbose;
  }

  /**
   * Examine the path metadata and verify that the dir is authoritative.
   * @param md metadata.
   * @param requireAuth require all directories to be authoritative
   * @throws NonAuthoritativeDirException if it is !auth and requireAuth=true.
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
             new DurationInfo(LOG, "Audit %s", path)) {
      return executeAudit(path, requireAuthoritative, true);
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
  Pair<Integer, Integer> executeAudit(
      final Path path,
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
      final Path p = dir.getFileStatus().getPath();
      LOG.debug("Directory {}", dir.prettyPrint());
      // log a message about the dir state, with root treated specially
      if (!p.isRoot()) {
        if (!dir.isAuthoritativeDir()) {
          LOG.warn("Directory {} is not authoritative", p);
          nonauth++;
          verifyAuthDir(dir, requireAuth);
        } else {
          LOG.info("Directory {}", p);
        }
      } else {
        // this is done to avoid the confusing message about root not being
        // authoritative
        LOG.info("Root directory {}", p);
      }

      // list its children
      if (recursive) {
        final DirListingMetadata entry = metastore.listChildren(p);

        if (entry != null) {
          final Collection<PathMetadata> listing = entry.getListing();
          int files = 0, subdirs = 0;
          for (PathMetadata e : listing) {
            if (isDirectory(e)) {
              // queue for auditing
              queue.add((DDBPathMetadata) e);
              subdirs++;
            } else {
              files++;
            }
          }
          if (verbose && files > 0 || subdirs > 0) {
            LOG.info("  files {}; directories {}", files, subdirs);
          }
        } else {
          LOG.info("Directory {} has been deleted", dir);
        }
      }
    }
    // end of scan
    if (dirs == 1 && isRoot) {
      LOG.info("The store has no directories to scan");
    } else {
      LOG.info("Scanned {} directories - {} were not marked as authoritative",
          dirs, nonauth);
    }
    return Pair.of(dirs, nonauth);
  }

  /**
   * A directory was found which was non-authoritative.
   * The exit code for this operation is
   * {@link LauncherExitCodes#EXIT_NOT_ACCEPTABLE} -This is what the S3Guard
   * will return.
   */
  public static final class NonAuthoritativeDirException
      extends PathIOException implements ExitCodeProvider {

    /**
     * Instantiate.
     * @param path the path which is non-authoritative.
     */
    private NonAuthoritativeDirException(final Path path) {
      super(path.toString(), E_NONAUTH);
    }

    @Override
    public int getExitCode() {
      return ERROR_ENTRY_NOT_AUTH_IN_DDB;
    }

    @Override
    public String toString() {
      return getMessage();
    }
  }

}
