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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.JsonSerialization;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.assertDirEntryMatch;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.assertFileEntryMatch;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.marshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.unmarshallPath;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Reading/writing manifest files.
 */
public class TestTaskManifestFileIO extends AbstractManifestCommitterTest {

  private TaskManifest source;

  private ManifestCommitterTestSupport.JobAndTaskIDsForTests taskIDs;

  private String taskAttempt00;

  private Path testPath;

  private Path taPath;

  @Override
  public void setup() throws Exception {
    super.setup();
    taskIDs = new ManifestCommitterTestSupport.JobAndTaskIDsForTests(2, 2);
    source = new TaskManifest();
    taskAttempt00 = taskIDs.getTaskAttempt(0, 0);
    source.setTaskAttemptID(taskAttempt00);
    testPath = methodPath();
    taPath = new Path(testPath, "  " + taskAttempt00);
    source.setTaskAttemptDir(marshallPath(taPath));
  }

  /**
   * Test marshalling, paying attention to paths with spaces in them
   * as they've been a source of trouble in the S3A committers.
   */
  @Test
  public void testJsonRoundTrip() throws Throwable {
    describe("Save manifest file to string and back");
    Path subdirS = new Path(taPath, "subdir");
    Path subdirD = new Path(testPath, "subdir");
    source.addDirectory(DirEntry.dirEntry(subdirD, 0, 0));

    // a file
    Path subfileS = new Path(subdirS, "file");
    Path subfileD = new Path(subdirD, "file");
    long len = 256L;
    FileEntry subFileEntry = new FileEntry(subfileS,
        subfileD, len, "etag");
    source.addFileToCommit(subFileEntry);


    JsonSerialization<TaskManifest> serializer
        = TaskManifest.serializer();

    String json = serializer.toJson(source);
    LOG.info("serialized form\n{}", json);
    TaskManifest deser = serializer.fromJson(json);
    deser.validate();

    Assertions.assertThat(deser.getTaskAttemptID())
        .describedAs("Task attempt ID")
        .isEqualTo(taskAttempt00);

    Assertions.assertThat(unmarshallPath(deser.getTaskAttemptDir()))
        .describedAs("Task attempt Dir %s",
            deser.getTaskAttemptDir())
        .isEqualTo(taPath);

    Assertions.assertThat(deser.getDestDirectories())
        .hasSize(1)
        .allSatisfy(d -> assertDirEntryMatch(d, subdirD, 0));
    Assertions.assertThat(deser.getFilesToCommit())
        .hasSize(1)
        .allSatisfy(d -> assertFileEntryMatch(d, subfileS, subfileD, len));
    final FileEntry entry = deser.getFilesToCommit().get(0);
    assertFileEntryMatch(entry, subfileS, subfileD, len);
    Assertions.assertThat(entry.getEtag())
        .describedAs("etag of %s", entry)
        .isEqualTo("etag");

  }

  /**
   * The manifest validation logic has a safety check that only one
   * file can rename to the same destination, and that the entries
   * are valid.
   */
  @Test
  public void testValidateRejectsTwoCommitsToSameDest() throws Throwable {

    Path subdirS = new Path(taPath, "subdir");
    Path subdirD = new Path(testPath, "subdir");
    source.addDirectory(DirEntry.dirEntry(subdirD, 0, 0));

    // a file
    Path subfileS = new Path(subdirS, "file");
    Path subfileS2 = new Path(subdirS, "file2");
    Path subfileD = new Path(subdirD, "file");
    long len = 256L;
    source.addFileToCommit(
        new FileEntry(subfileS, subfileD, len, "tag1"));
    source.addFileToCommit(
        new FileEntry(subfileS2, subfileD, len, "tag2"));
    assertValidationFailureOnRoundTrip(source);
  }

  /**
   * The manifest validation logic has a safety check that only one
   * file can rename to the same destination, and that the entries
   * are valid.
   */
  @Test
  public void testValidateRejectsIncompleteFileEntry() throws Throwable {
    source.addFileToCommit(
        new FileEntry(taPath, null, 0, null));
    assertValidationFailureOnRoundTrip(source);
  }

  /**
   * negative lengths are not allowed.
   */
  @Test
  public void testValidateRejectsInvalidFileLength() throws Throwable {
    source.addFileToCommit(
        new FileEntry(taPath, testPath, -1, null));
    assertValidationFailureOnRoundTrip(source);
  }

  @Test
  public void testRejectIncompatibleVersion() throws Throwable {
    source.setVersion(5);
    assertValidationFailureOnRoundTrip(source);
  }

  @Test
  public void testRejectIncompatibleType() throws Throwable {
    source.setType("Incompatible type");
    assertValidationFailureOnRoundTrip(source);
  }

  private void assertValidationFailureOnRoundTrip(
      final TaskManifest manifest) throws Exception {
    JsonSerialization<TaskManifest> serializer
        = TaskManifest.serializer();
    String json = serializer.toJson(manifest);
    LOG.info("serialized form\n{}", json);
    TaskManifest deser = serializer.fromJson(json);
    intercept(IOException.class, deser::validate);
  }


}
