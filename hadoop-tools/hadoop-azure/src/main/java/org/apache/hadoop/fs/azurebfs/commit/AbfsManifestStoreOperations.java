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

package org.apache.hadoop.fs.azurebfs.commit;

import java.io.IOException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperationsThroughFileSystem;

/**
 * Extension of StoreOperationsThroughFileSystem with ABFS awareness.
 * Purely for use by jobs committing work through the manifest committer.
 * The {@link AzureManifestCommitterFactory} will configure
 * this as the binding to the FS.
 *
 * ADLS Gen2 stores support etag-recovery on renames, but not WASB
 * stores.
 */
@InterfaceAudience.LimitedPrivate("mapreduce")
@InterfaceStability.Unstable
public class AbfsManifestStoreOperations extends
    ManifestStoreOperationsThroughFileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsManifestStoreOperations.class);

  /**
   * Classname, which can be declared in jpb configurations.
   */
  public static final String NAME = AbfsManifestStoreOperations.class.getName();

  /**
   * Resilient rename calls; only available on an ADLS Gen2 store.
   * Will be null after binding if the FS isn't compatible.
   */
  private ResilientCommitByRename resilientCommitByRename;

  /**
   * Are etags preserved in renames?
   */
  private boolean etagsPreserved;

  @Override
  public AzureBlobFileSystem getFileSystem() {
    return (AzureBlobFileSystem) super.getFileSystem();
  }

  /**
   * Bind to the store.
   *
   * @param filesystem FS.
   * @param path path to work under
   * @throws IOException binding problems.
   */
  @Override
  public void bindToFileSystem(FileSystem filesystem, Path path) throws IOException {
    if (!(filesystem instanceof AzureBlobFileSystem)) {
      throw new PathIOException(path.toString(),
          "Not an abfs filesystem: " + filesystem.getClass());
    }
    super.bindToFileSystem(filesystem, path);
    try {
      resilientCommitByRename = getFileSystem().createResilientCommitSupport(path);
      // this also means that etags are preserved.
      etagsPreserved = true;
      LOG.debug("Bonded to filesystem with resilient commits under path {}", path);
    } catch (UnsupportedOperationException e) {
      LOG.debug("No resilient commit support under path {}", path);
    }
  }

  /**
   * Etags are preserved through Gen2 stores, but not wasb stores.
   * @param path path to probe.
   * @return true if this store preserves etags.
   */
  @Override
  public boolean storePreservesEtagsThroughRenames(final Path path) {
    return etagsPreserved;
  }

  /**
   * Resilient commits available on hierarchical stores.
   * @return true if the FS can use etags on renames.
   */
  @Override
  public boolean storeSupportsResilientCommit() {
    return resilientCommitByRename != null;
  }

  /**
   * Commit a file through an internal ABFS operation.
   * If resilient commit is unavailable, invokes the superclass, which
   * will raise an UnsupportedOperationException
   * @param entry entry to commit
   * @return the outcome
   * @throws IOException any failure in resilient commit.
   * @throws UnsupportedOperationException if not available.
   */
  @Override
  public CommitFileResult commitFile(final FileEntry entry) throws IOException {

    if (resilientCommitByRename != null) {
      final Pair<Boolean, Duration> result =
          resilientCommitByRename.commitSingleFileByRename(
              entry.getSourcePath(),
              entry.getDestPath(),
              entry.getEtag());
      return CommitFileResult.fromResilientCommit(result.getLeft(),
          result.getRight());
    } else {
      return super.commitFile(entry);
    }
  }
}
