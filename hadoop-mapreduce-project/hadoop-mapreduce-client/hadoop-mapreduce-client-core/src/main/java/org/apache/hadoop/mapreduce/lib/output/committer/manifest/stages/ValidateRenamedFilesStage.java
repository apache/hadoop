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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.EntryFileIO;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.OutputValidationException;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_VALIDATE_OUTPUT;

/**
 * This stage validates all files by scanning the manifests
 * and verifying every file in every manifest is of the given size.
 * Returns a list of all files committed.
 *
 * Its cost is one getFileStatus() call (parallelized) per file.
 * Raises a {@link OutputValidationException} on a validation failure.
 */
public class ValidateRenamedFilesStage extends
    AbstractJobOrTaskStage<
        Path,
        List<FileEntry>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      ValidateRenamedFilesStage.class);

  /**
   * List of all files committed.
   */
  private List<FileEntry> filesCommitted = new ArrayList<>();

  public ValidateRenamedFilesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_VALIDATE_OUTPUT, true);
  }

  /**
   * Get the list of files committed.
   * @return a possibly empty list.
   */
  private synchronized List<FileEntry> getFilesCommitted() {
    return filesCommitted;
  }

  /**
   * Add a file entry to the list of committed files.
   * @param entry entry
   */
  private synchronized void addFileCommitted(FileEntry entry) {
    filesCommitted.add(entry);
  }

  /**
   * Validate the task manifests.
   * This is done by listing all the directories
   * and verifying that every file in the source list
   * has a file in the destination of the same size.
   * If two tasks have both written the same file or
   * a source file was changed after the task was committed,
   * then a mismatch will be detected -provided the file
   * length is now different.
   * @param entryFile path to entry file
   * @return list of files committed.
   */
  @Override
  protected List<FileEntry> executeStage(
      final Path entryFile)
      throws IOException {

    final EntryFileIO entryFileIO = new EntryFileIO(getStageConfig().getConf());

    try (SequenceFile.Reader reader = entryFileIO.createReader(entryFile)) {
      // iterate over the entries in the file.
      TaskPool.foreach(entryFileIO.iterateOver(reader))
          .executeWith(getIOProcessors())
          .stopOnFailure()
          .run(this::validateOneFile);

      return getFilesCommitted();
    }
  }

  /**
   * Validate a file.
   * @param entry entry to probe for
   * @throws IOException IO problem.
   * @throws OutputValidationException if the entry is not valid
   */
  private void validateOneFile(FileEntry entry) throws IOException {
    updateAuditContext(OP_STAGE_JOB_VALIDATE_OUTPUT);

    // report progress back
    progress();
    // look validate the file.
    // raising an FNFE if the file isn't there.
    FileStatus destStatus;
    final Path sourcePath = entry.getSourcePath();
    Path destPath = entry.getDestPath();
    try {
      destStatus = getFileStatus(destPath);

      // it must be a file
      if (!destStatus.isFile()) {
        throw new OutputValidationException(destPath,
            "Expected a file renamed from " + sourcePath
                + "; found " + destStatus);
      }
      final long sourceSize = entry.getSize();
      final long destSize = destStatus.getLen();

      // etags, if the source had one.
      final String sourceEtag = entry.getEtag();
      if (getOperations().storePreservesEtagsThroughRenames(destStatus.getPath())
          && isNotBlank(sourceEtag)) {
        final String destEtag = ManifestCommitterSupport.getEtag(destStatus);
        if (!sourceEtag.equals(destEtag)) {
          LOG.warn("Etag of dest file {}: {} does not match that of manifest entry {}",
              destPath, destStatus, entry);
          throw new OutputValidationException(destPath,
              String.format("Expected the file"
                      + " renamed from %s"
                      + " with etag %s and length %s"
                      + " but found a file with etag %s and length %d",
                  sourcePath,
                  sourceEtag,
                  sourceSize,
                  destEtag,
                  destSize));

        }
      }
      // check the expected length after any etag validation
      if (destSize != sourceSize) {
        LOG.warn("Length of dest file {}: {} does not match that of manifest entry {}",
            destPath, destStatus, entry);
        throw new OutputValidationException(destPath,
            String.format("Expected the file"
                    + " renamed from %s"
                    + " with length %d"
                    + " but found a file of length %d",
                sourcePath,
                sourceSize,
                destSize));
      }

    } catch (FileNotFoundException e) {
      // file didn't exist
      throw new OutputValidationException(destPath,
          "Expected a file, but it was not found", e);
    }
    addFileCommitted(entry);
  }

}
