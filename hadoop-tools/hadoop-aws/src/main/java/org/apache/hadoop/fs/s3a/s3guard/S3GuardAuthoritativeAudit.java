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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;

/**
 * Audit a directory tree for being authoritative.
 */
public class S3GuardAuthoritativeAudit {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3GuardAuthoritativeAudit.class);

  public static final int ERROR_ENTRY_NOT_AUTH_IN_DDB = 4;
  public static final int ERROR_PATH_NOT_AUTH_IN_FS = 5;

  private final StoreContext storeContext;

  private final DynamoDBMetadataStore metastore;

  public S3GuardAuthoritativeAudit(final StoreContext storeContext,
      final DynamoDBMetadataStore metastore) {
    this.storeContext = storeContext;
    this.metastore = metastore;
  }

  /**
   * Examine the path metadata, declare whether it should be queued for
   * recursive scanning.
   * @param md metadata.
   * @return true if it is a dir to scan.
   * @throws ExitUtil.ExitException if it is a non-auth dir.
   */
  private boolean isAuthDir(DDBPathMetadata md) {
    if (md.getFileStatus().isFile()) {
      // file: exist without a check
      return false;
    }
    // directory - require authoritativeness
    if (!md.isAuthoritativeDir()) {
      throw new ExitUtil.ExitException(ERROR_ENTRY_NOT_AUTH_IN_DDB,
          "Directory is not marked as authoritative in the S3Guard store: "
              + md.getFileStatus().getPath());
    }
    // we are an authoritative dir
    return true;
  }

  /**
   * Audit the tree.
   * @param path path to scan
   * @return count of dirs scanned. 0 == path was a file.
   * @throws IOException IO failure
   * @throws ExitUtil.ExitException if a non-auth dir was found.
   */
  public int audit(Path path) throws IOException {
    final Path qualified = storeContext.qualify(path);
    LOG.info("Auditing {}", qualified);
    try (DurationInfo d = new DurationInfo(LOG, "audit %s", qualified)) {
      return executeAudit(qualified);
    }
  }

  /**
   * Audit the tree.
   * @param path path to scan
   * @return count of dirs scanned. 0 == path was a file.
   * @throws IOException IO failure
   * @throws ExitUtil.ExitException if a non-auth dir was found.
   */
  private int executeAudit(Path path) throws IOException {
    final Queue<DDBPathMetadata> queue = new ArrayDeque<>();
    final boolean isRoot = path.isRoot();
    final DDBPathMetadata baseData = metastore.get(path);
    if (baseData == null) {
      throw new ExitUtil.ExitException(LauncherExitCodes.EXIT_NOT_FOUND,
          "No S3Guard entry for path " + path);
    }

    if (isRoot || isAuthDir(baseData)) {
      // we have the root entry or an authoritative a directory
      queue.add(baseData);
    } else {
      LOG.info("Path represents file");
      return 0;
    }

    int count = 0;
    while (!queue.isEmpty()) {
      count++;
      final DDBPathMetadata dir = queue.poll();
      LOG.info("Directory {}", dir.getFileStatus().getPath());
      LOG.debug("Directory {}", dir);

      // list its children
      final DirListingMetadata entry = metastore.listChildren(
          dir.getFileStatus().getPath());

      if (entry != null) {
        final Collection<PathMetadata> listing = entry.getListing();
        int files = 0, dirs = 0;
        for (PathMetadata e : listing) {
          if (isAuthDir((DDBPathMetadata) e)) {
            queue.add((DDBPathMetadata) e);
            dirs++;
          } else {
            files++;
          }
        }
        LOG.info("  files {}; directories {}", files, dirs);
      } else {
        LOG.info("Directory {} has been deleted", dir);
      }
    }
    return count;
  }
}
