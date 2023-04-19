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

import java.io.File;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.LoadManifestsStage;

import static java.util.Objects.requireNonNull;

/**
 * Information about the loaded manifest data;
 * Returned from {@link LoadManifestsStage} and then
 * used for renaming the work.
 */
public final class LoadedManifestData {

  /**
   * Directories.
   */
  private final Collection<DirEntry> directories;

  /**
   * Path of the intermediate cache of
   * files to rename.
   * This will be a sequence file of long -> FileEntry
   */
  private final Path entrySequenceData;

  /**
   * How many files will be renamed.
   */
  private final int fileCount;

  /**
   * Data about the loaded manifests.
   * @param directories directories
   * @param entrySequenceData Path in local fs to the entry sequence data.
   * @param fileCount number of files.
   */
  public LoadedManifestData(
      final Collection<DirEntry> directories,
      final Path entrySequenceData,
      final int fileCount) {
    this.directories = requireNonNull(directories);
    this.fileCount = fileCount;
    this.entrySequenceData = requireNonNull(entrySequenceData);
  }

  public Collection<DirEntry> getDirectories() {
    return directories;
  }

  public int getFileCount() {
    return fileCount;
  }

  /**
   * Get the path to the entry sequence data file.
   * @return the path
   */
  public Path getEntrySequenceData() {
    return entrySequenceData;
  }

  /**
   * Get the entry sequence data as a file.
   */
  public File getEntrySequenceFile() {
    return new File(entrySequenceData.toUri());
  }

  /**
   * Delete the entry sequence file.
   * @return whether or not the delete was successful.
   */
  public boolean deleteEntrySequenceFile() {
    return getEntrySequenceFile().delete();
  }
}
