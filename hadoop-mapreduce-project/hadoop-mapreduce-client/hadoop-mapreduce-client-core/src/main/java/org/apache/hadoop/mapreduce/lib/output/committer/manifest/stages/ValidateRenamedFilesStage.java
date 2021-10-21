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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.OutputValidationException;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_VALIDATE_OUTPUT;
import static org.apache.hadoop.thirdparty.com.google.common.collect.Iterables.concat;

/**
 * This stage validates all files by scanning the manifests
 * and verifying every file in every manifest is of the given size.
 * Returns a list of all files committed.
 *
 * Its cost is one getFileStatus() call (parallelized) per file.
 * Raises a {@link OutputValidationException} on a validation failure.
 */
public class ValidateRenamedFilesStage extends
    AbstractJobCommitStage<List<TaskManifest>, List<FileOrDirEntry>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      ValidateRenamedFilesStage.class);

  /**
   * Set this to halt all workers.
   */
  private final AtomicBoolean halt = new AtomicBoolean();

  /**
   * List of all files committed.
   */
  private List<FileOrDirEntry> filesCommitted = new ArrayList<>();

  public ValidateRenamedFilesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_VALIDATE_OUTPUT, true);
  }

  /**
   * Get the list of files committed.
   * @return a possibly empty list.
   */
  private synchronized List<FileOrDirEntry> getFilesCommitted() {
    return filesCommitted;
  }

  /**
   * Add a file entry to the list of committed files.
   * @param entry entry
   */
  private synchronized void addFileCommitted(FileOrDirEntry entry) {
    filesCommitted.add(entry);
  }

  /**
   * Validate the task manifests.
   * This is done by listing all the directories
   * and verifying that every file in the source list
   * has a file in the destination of the same size.
   * If two tasks have both written the same file or
   * a source file was changed after the task was committed,
   * then a mistmatch will be detected -provided the file
   * length is now different.
   * @param taskManifests list of manifests.
   * @return list of files committed.
   */
  @Override
  protected List<FileOrDirEntry> executeStage(
      final List<TaskManifest> taskManifests)
      throws IOException {

    // set the list of files to be as big as the number of tasks.
    // synchronized to stop complaints.
    synchronized (this) {
      filesCommitted = new ArrayList<>(taskManifests.size());
    }

    // validate all the files.

    final Iterable<FileOrDirEntry> filesToCommit = concat(taskManifests.stream()
        .map(TaskManifest::getFilesToCommit)
        .collect(Collectors.toList()));

    TaskPool.foreach(filesToCommit)
        .executeWith(getIOProcessors())
        .stopOnFailure()
        .run(this::validateOneFile);

    return getFilesCommitted();
  }

  /**
   * Validate a file.
   * @param entry entry to probe for
   * @throws IOException IO problem.
   * @throws OutputValidationException if the entry is not valid
   */
  private void validateOneFile(FileOrDirEntry entry) throws IOException {
    updateAuditContext(OP_STAGE_JOB_VALIDATE_OUTPUT);

    if (halt.get()) {
      // told to stop
      return;
    }
    // report progress back
    progress();
    // look validate the file.
    // raising an FNFE if the file isn't there.
    FileStatus st = null;
    Path path = entry.getDestPath();
    try {
      st = getFileStatus(path);

      // it must be a file
      if (!st.isFile()) {
        throw new OutputValidationException(path,
            "Expected a file, found " + st);
      }
      // of the expected length
      if (st.getLen() != entry.getSize()) {
        throw new OutputValidationException(path,
            String.format("Expected a file of length %s"
                    + " but found a file of length %s",
                entry.getSize(),
                st.getLen()));
      }
    } catch (FileNotFoundException e) {
      // file didn't exist
      throw new OutputValidationException(path,
          "Expected a file, but it was not found", e);
    }
    addFileCommitted(entry);
  }

}
