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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.JsonSerialization;

/**
 * Implementation of manifest store operations through the filesystem API.
 * This class is subclassed in the ABFS module, which does add the resilient
 * commit method.
 */
@InterfaceAudience.LimitedPrivate("mapreduce, object-stores")
@InterfaceStability.Unstable
public class ManifestStoreOperationsThroughFileSystem extends ManifestStoreOperations {

  /**
   * Filesystem; set in {@link #bindToFileSystem(FileSystem, Path)}.
   */
  private FileSystem fileSystem;

  /**
   * Has a call to FileSystem.msync() failed as unsupported?
   * If so, no new attempts will be made when
   * (@link {@link #msync(Path)} is invoked.
   */
  private boolean msyncUnsupported = false;

  /**
   * Direct Constructor.
   * @param fileSystem filesystem to write through.
   */
  public ManifestStoreOperationsThroughFileSystem(final FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  /**
   * Constructor used for introspection-based binding.
   */
  public ManifestStoreOperationsThroughFileSystem() {
  }

  @Override
  public void close() throws IOException {
    /* no-op; FS is assumed to be shared. */

  }

  /**
   * Get the filesystem.
   * @return the filesystem; null until bound.
   */
  public FileSystem getFileSystem() {
    return fileSystem;
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

  /**
   * Probe filesystem capabilities.
   * @param path path to probe.
   * @return true if the FS declares its renames work.
   */
  @Override
  public boolean storePreservesEtagsThroughRenames(Path path) {
    try {
      return fileSystem.hasPathCapability(path,
          CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME);
    } catch (IOException ignored) {
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

}