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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.JsonSerialization;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;

/**
 * Implement task and job operations through the filesystem API.
 */
public class StoreOperationsThroughFileSystem implements StoreOperations {

  private static final Logger LOG = LoggerFactory.getLogger(
      StoreOperationsThroughFileSystem.class);

  /**
   * Trash.moveToTrash() returned false.
   */
  public static final String E_TRASH_FALSE = "Failed to rename to trash" +
      " -check trash interval in " + FS_TRASH_INTERVAL_KEY +": ";

  /**
   * Filesystem; set in {@link #bindToFileSystem(FileSystem, Path)}.
   */
  private FileSystem fileSystem;

  /**
   * Has a call to msync failed as unsupported?
   */
  private boolean msyncUnsupported = false;

  /**
   * Constructor.
   * @param fileSystem filesystem to write through.
   */
  public StoreOperationsThroughFileSystem(final FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  /**
   * Constructor used for introspection-based binding.
   */
  public StoreOperationsThroughFileSystem() {
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void bindToFileSystem(FileSystem filesystem, Path path) throws IOException {
    fileSystem = filesystem;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return fileSystem.getFileStatus(path);
  }

  /**
   * Using FileSystem.isFile to offer stores the option to optimize their probes.
   * @param path path to probe
   * @return true if the path resolves to a file.
   * @throws IOException IO failure.
   */
  @SuppressWarnings("deprecation")
  @Override
  public boolean isFile(Path path) throws IOException {
    return fileSystem.isFile(path);
  }

  @Override
  public boolean delete(Path path, boolean recursive)
      throws IOException {
    return fileSystem.delete(path, recursive);
  }

  @Override
  public boolean mkdirs(Path path)
      throws IOException {
    return fileSystem.mkdirs(path);
  }

  @Override
  public boolean renameFile(Path source, Path dest)
      throws IOException {
    return fileSystem.rename(source, dest);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException {
    return fileSystem.listStatusIterator(path);
  }

  @Override
  public TaskManifest loadTaskManifest(
      JsonSerialization<TaskManifest> serializer,
      FileStatus st) throws IOException {
    return TaskManifest.load(serializer, fileSystem, st.getPath(), st);
  }

  @Override
  public <T extends AbstractManifestData<T>> void save(
      final T manifestData,
      final Path path,
      final boolean overwrite) throws IOException {
    manifestData.save(fileSystem, path, overwrite);
  }

  @Override
  public boolean isTrashEnabled(Path path) {
    try {
      return fileSystem.getServerDefaults(path).getTrashInterval() > 0;
    } catch (IOException e) {
      // catch and downgrade to false.
      // trash is clealy broken.
      LOG.info("getServerDefaults({}) failed", path, e);
      return false;
    }
  }

  /**
   * Invokes FileSystem msync(); swallows UnsupportedOperationExceptions.
   * This ensures client metadata caches are in sync in an HDFS-HA deployment.
   * No other filesystems support this; in the absence of a hasPathCapability()
   * probe, after the operation is rejected, an atomic boolean is set
   * to stop further attempts from even trying.
   * @param path path
   * @throws IOException failure to synchronize.
   */
  @Override
  public void msync(Path path) throws IOException {
    // there's need for atomicity here, as the sole cost of
    // multiple failures
    if (msyncUnsupported) {
      return;
    }
    // qualify so we can be confident that the FS being synced
    // is the one we expect.
    fileSystem.makeQualified(path);
    try {
      fileSystem.msync();
    } catch (UnsupportedOperationException ignored) {
      // this exception is the default.
      // set the unsupported flag so no future attempts are made.
      msyncUnsupported = true;
    }
  }

  /**
   * etag extract is not available in the base store.
   * @param status status, which may be of any subclass of FileStatus.
   * @return null.
   */
  @Override
  public String getEtag(FileStatus status) {
    return null;
  }

  /**
   * Move a dir/file to the user's trash dir under the jobID.
   * IOExceptions in rename are caught, logged at info
   * and then downgraded to a "return false"
   * @param jobId job ID.
   * @param path path to move, assumed to be _temporary
   * @return true if the rename succeeded.
   */
  @Override
  public MoveToTrashResult moveToTrash(String jobId, Path path) {

    MoveToTrashOutcome outcome;
    IOException ioe = null;
    try {
      boolean renamed = Trash.moveToAppropriateTrash(fileSystem,
          path, fileSystem.getConf());
      if (!renamed) {
        ioe = new PathIOException(path.toString(),
            E_TRASH_FALSE + fileSystem.getServerDefaults(
                path).getTrashInterval());
        outcome = MoveToTrashOutcome.FAILURE;
      } else {
        outcome = MoveToTrashOutcome.RENAMED_TO_TRASH;
      }
    } catch (IOException ex) {
      outcome = MoveToTrashOutcome.FAILURE;
      LOG.info("Failed to move {} to trash: {}",
          path, ex.toString());
      LOG.debug("Full stack", ex);
      ioe = ex;
    }
    return new MoveToTrashResult(outcome, ioe);
  }

}