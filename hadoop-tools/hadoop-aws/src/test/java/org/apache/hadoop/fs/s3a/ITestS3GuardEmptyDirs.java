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

package org.apache.hadoop.fs.s3a;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test logic around whether or not a directory is empty, with S3Guard enabled.
 * The fact that S3AFileStatus has an isEmptyDirectory flag in it makes caching
 * S3AFileStatus's really tricky, as the flag can change as a side effect of
 * changes to other paths.
 * After S3Guard is merged to trunk, we should try to remove the
 * isEmptyDirectory flag from S3AFileStatus, or maintain it outside
 * of the MetadataStore.
 */
public class ITestS3GuardEmptyDirs extends AbstractS3ATestBase {

  @Test
  public void testRenameEmptyDir() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path basePath = path(getMethodName());
    Path sourceDir = new Path(basePath, "AAA-source");
    String sourceDirMarker = fs.pathToKey(sourceDir) + "/";
    Path destDir = new Path(basePath, "BBB-dest");
    String destDirMarker = fs.pathToKey(destDir) + "/";
    // set things up.
    mkdirs(sourceDir);
    // there'a source directory marker
    fs.getObjectMetadata(sourceDirMarker);
    S3AFileStatus srcStatus = getEmptyDirStatus(sourceDir);
    assertEquals("Must be an empty dir: " + srcStatus, Tristate.TRUE,
        srcStatus.isEmptyDirectory());
    // do the rename
    assertRenameOutcome(fs, sourceDir, destDir, true);
    S3AFileStatus destStatus = getEmptyDirStatus(destDir);
    assertEquals("Must be an empty dir: " + destStatus, Tristate.TRUE,
        destStatus.isEmptyDirectory());
    // source does not exist.
    intercept(FileNotFoundException.class,
        () -> getEmptyDirStatus(sourceDir));
    // and verify that there's no dir marker hidden under a tombstone
    intercept(FileNotFoundException.class,
        () -> Invoker.once("HEAD", sourceDirMarker,
            () -> fs.getObjectMetadata(sourceDirMarker)));
    // the parent dir mustn't be confused
    S3AFileStatus baseStatus = getEmptyDirStatus(basePath);
    assertEquals("Must not be an empty dir: " + baseStatus, Tristate.FALSE,
        baseStatus.isEmptyDirectory());
    // and verify the dest dir has a marker
    fs.getObjectMetadata(destDirMarker);
  }

  private S3AFileStatus getEmptyDirStatus(Path dir) throws IOException {
    return getFileSystem().innerGetFileStatus(dir, true);
  }

  @Test
  public void testEmptyDirs() throws Exception {
    S3AFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.hasMetadataStore());
    MetadataStore configuredMs = fs.getMetadataStore();
    Path existingDir = path("existing-dir");
    Path existingFile = path("existing-dir/existing-file");
    try {
      // 1. Simulate files already existing in the bucket before we started our
      // cluster.  Temporarily disable the MetadataStore so it doesn't witness
      // us creating these files.

      fs.setMetadataStore(new NullMetadataStore());
      assertTrue(fs.mkdirs(existingDir));
      touch(fs, existingFile);


      // 2. Simulate (from MetadataStore's perspective) starting our cluster and
      // creating a file in an existing directory.
      fs.setMetadataStore(configuredMs);  // "start cluster"
      Path newFile = path("existing-dir/new-file");
      touch(fs, newFile);

      S3AFileStatus status = fs.innerGetFileStatus(existingDir, true);
      assertEquals("Should not be empty dir", Tristate.FALSE,
          status.isEmptyDirectory());

      // 3. Assert that removing the only file the MetadataStore witnessed
      // being created doesn't cause it to think the directory is now empty.
      fs.delete(newFile, false);
      status = fs.innerGetFileStatus(existingDir, true);
      assertEquals("Should not be empty dir", Tristate.FALSE,
          status.isEmptyDirectory());

      // 4. Assert that removing the final file, that existed "before"
      // MetadataStore started, *does* cause the directory to be marked empty.
      fs.delete(existingFile, false);
      status = fs.innerGetFileStatus(existingDir, true);
      assertEquals("Should be empty dir now", Tristate.TRUE,
          status.isEmptyDirectory());
    } finally {
      configuredMs.forgetMetadata(existingFile);
      configuredMs.forgetMetadata(existingDir);
    }
  }
}
