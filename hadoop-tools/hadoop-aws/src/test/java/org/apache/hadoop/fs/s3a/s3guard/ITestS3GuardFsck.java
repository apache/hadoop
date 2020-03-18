/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;


import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.junit.Assume.assumeTrue;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.awaitFileStatus;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Integration tests for the S3Guard Fsck against a dyamodb backed metadata
 * store.
 */
public class ITestS3GuardFsck extends AbstractS3ATestBase {

  private S3AFileSystem guardedFs;
  private S3AFileSystem rawFs;

  private MetadataStore metadataStore;

  @Before
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    // These test will fail if no ms
    assumeTrue("FS needs to have a metadatastore.",
        fs.hasMetadataStore());
    assumeTrue("Metadatastore should persist authoritative bit",
        metadataStorePersistsAuthoritativeBit(fs.getMetadataStore()));

    guardedFs = fs;
    metadataStore = fs.getMetadataStore();

    // create raw fs without s3guard
    rawFs = createUnguardedFS();
    assertFalse("Raw FS still has S3Guard " + rawFs,
        rawFs.hasMetadataStore());
  }

  @Override
  public void teardown() throws Exception {
    if (guardedFs != null) {
      IOUtils.cleanupWithLogger(LOG, guardedFs);
    }
    IOUtils.cleanupWithLogger(LOG, rawFs);
    super.teardown();
  }

  /**
   * Create a test filesystem which is always unguarded.
   * This filesystem MUST be closed in test teardown.
   * @return the new FS
   */
  private S3AFileSystem createUnguardedFS() throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration config = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();

    removeBaseAndBucketOverrides(uri.getHost(), config,
        S3_METADATA_STORE_IMPL, METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    S3AFileSystem fs2 = new S3AFileSystem();
    fs2.initialize(uri, config);
    return fs2;
  }

  @Test
  public void testIDetectNoMetadataEntry() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      touchRawAndWaitRaw(file);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 2);
      final S3GuardFsck.ComparePair pair = comparePairs.get(0);
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.NO_METADATA_ENTRY);
    } finally {
      // delete the working directory with all of its contents
      cleanup(file, cwd);
    }
  }

  @Test
  public void testIDetectNoParentEntry() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      touchGuardedAndWaitRaw(file);
      // delete the parent from the MS
      metadataStore.forgetMetadata(cwd);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 2);
      // check the parent that it does not exist
      checkForViolationInPairs(cwd, comparePairs,
          S3GuardFsck.Violation.NO_METADATA_ENTRY);
      // check the child that there's no parent entry.
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.NO_PARENT_ENTRY);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testIDetectParentIsAFile() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      touchGuardedAndWaitRaw(file);
      // modify the cwd metadata and set that it's not a directory
      final S3AFileStatus newParentFile = MetadataStoreTestBase
          .basicFileStatus(cwd, 1, false, 1);
      metadataStore.put(new PathMetadata(newParentFile));

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 2);
      // check the parent that it does not exist
      checkForViolationInPairs(cwd, comparePairs,
          S3GuardFsck.Violation.DIR_IN_S3_FILE_IN_MS);
      // check the child that the parent is a file.
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.PARENT_IS_A_FILE);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testIDetectParentTombstoned() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      touchGuardedAndWaitRaw(file);
      // modify the parent metadata and set that it's not a directory
      final PathMetadata cwdPmd = metadataStore.get(cwd);
      cwdPmd.setIsDeleted(true);
      metadataStore.put(cwdPmd);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      // check the child that the parent is tombstoned
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.PARENT_TOMBSTONED);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testIDetectDirInS3FileInMs() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    try {
      // create a file with guarded fs
      mkdirs(cwd);
      awaitFileStatus(guardedFs, cwd);
      // modify the cwd metadata and set that it's not a directory
      final S3AFileStatus newParentFile = MetadataStoreTestBase
          .basicFileStatus(cwd, 1, false, 1);
      metadataStore.put(new PathMetadata(newParentFile));

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);
      assertComparePairsSize(comparePairs, 1);

      // check the child that the dir in s3 is a file in the ms
      checkForViolationInPairs(cwd, comparePairs,
          S3GuardFsck.Violation.DIR_IN_S3_FILE_IN_MS);
    } finally {
      cleanup(cwd);
    }
  }

  @Test
  public void testIDetectFileInS3DirInMs() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      touchGuardedAndWaitRaw(file);
      // modify the cwd metadata and set that it's not a directory
      final S3AFileStatus newFile = MetadataStoreTestBase
          .basicFileStatus(file, 1, true, 1);
      metadataStore.put(new PathMetadata(newFile));

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 1);
      // check the child that the dir in s3 is a file in the ms
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.FILE_IN_S3_DIR_IN_MS);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testIAuthoritativeDirectoryContentMismatch() throws Exception {
    assumeTrue("Authoritative directory listings should be enabled for this "
            + "test", guardedFs.hasAuthoritativeMetadataStore());
    // first dir listing will be correct
    final Path cwdCorrect = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path fileC1 = new Path(cwdCorrect, "fileC1");
    final Path fileC2 = new Path(cwdCorrect, "fileC2");

    // second dir listing will be incorrect: missing entry from Dynamo
    final Path cwdIncorrect = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path fileIc1 = new Path(cwdIncorrect, "fileC1");
    final Path fileIc2 = new Path(cwdIncorrect, "fileC2");
    try {
      touchGuardedAndWaitRaw(fileC1);
      touchGuardedAndWaitRaw(fileC2);
      touchGuardedAndWaitRaw(fileIc1);

      // get listing from ms and set it authoritative
      final DirListingMetadata dlmC = metadataStore.listChildren(cwdCorrect);
      final DirListingMetadata dlmIc = metadataStore.listChildren(cwdIncorrect);
      dlmC.setAuthoritative(true);
      dlmIc.setAuthoritative(true);
      metadataStore.put(dlmC, Collections.emptyList(), null);
      metadataStore.put(dlmIc, Collections.emptyList(), null);

      // add a file raw so the listing will be different.
      touchRawAndWaitRaw(fileIc2);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> pairsCorrect =
          s3GuardFsck.compareS3ToMs(cwdCorrect);
      final List<S3GuardFsck.ComparePair> pairsIncorrect =
          s3GuardFsck.compareS3ToMs(cwdIncorrect);

      // Assert that the correct dir does not contain the violation.
      assertTrue(pairsCorrect.stream()
          .noneMatch(p -> p.getPath().equals(cwdCorrect)));

      // Assert that the incorrect listing contains the violation.
      checkForViolationInPairs(cwdIncorrect, pairsIncorrect,
          S3GuardFsck.Violation.AUTHORITATIVE_DIRECTORY_CONTENT_MISMATCH);
    } finally {
      cleanup(fileC1, fileC2, fileIc1, fileIc2, cwdCorrect, cwdIncorrect);
    }
  }

  @Test
  public void testIDetectLengthMismatch() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touchGuardedAndWaitRaw(file);

      // modify the file metadata so the length will not match
      final S3AFileStatus newFile = MetadataStoreTestBase
          .basicFileStatus(file, 9999, false, 1);
      metadataStore.put(new PathMetadata(newFile));

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 1);
      // Assert that the correct dir does not contain the violation.
      assertTrue(comparePairs.stream()
          .noneMatch(p -> p.getPath().equals(cwd)));
      // Assert that the incorrect file meta contains the violation.
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.LENGTH_MISMATCH);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testIDetectModTimeMismatch() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touchGuardedAndWaitRaw(file);
      // modify the parent meta entry so the MOD_TIME will surely be up to date
      final FileStatus oldCwdFileStatus = rawFs.getFileStatus(cwd);
      final S3AFileStatus newCwdFileStatus = MetadataStoreTestBase
          .basicFileStatus(cwd, 0, true,
              oldCwdFileStatus.getModificationTime());
      metadataStore.put(new PathMetadata(newCwdFileStatus));

      // modify the file metadata so the length will not match
      final S3AFileStatus newFileStatus = MetadataStoreTestBase
          .basicFileStatus(file, 0, false, 1);
      metadataStore.put(new PathMetadata(newFileStatus));

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 1);
      // Assert that the correct dir does not contain the violation.
      assertTrue(comparePairs.stream()
          .noneMatch(p -> p.getPath().equals(cwd)));
      // check the file meta that there's a violation.
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.MOD_TIME_MISMATCH);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testIEtagMismatch() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      touchGuardedAndWaitRaw(file);
      // modify the file metadata so the etag will not match
      final S3AFileStatus newFileStatus = new S3AFileStatus(1, 1, file, 1, "",
          "etag", "versionId");
      metadataStore.put(new PathMetadata(newFileStatus));

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 1);
      // check the child that there's a BLOCKSIZE_MISMATCH
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.ETAG_MISMATCH);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testINoEtag() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file1 = new Path(cwd, "file1");
    final Path file2 = new Path(cwd, "file2");
    try {
      // create a file1 with guarded fs
      touchGuardedAndWaitRaw(file1);
      touchGuardedAndWaitRaw(file2);
      // modify the file1 metadata so there's no etag
      final S3AFileStatus newFile1Status =
          new S3AFileStatus(1, 1, file1, 1, "", null, "versionId");
      final S3AFileStatus newFile2Status =
          new S3AFileStatus(1, 1, file2, 1, "", "etag", "versionId");
      metadataStore.put(new PathMetadata(newFile1Status));
      metadataStore.put(new PathMetadata(newFile2Status));

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 2);

      // check file 1 that there's NO_ETAG
      checkForViolationInPairs(file1, comparePairs,
          S3GuardFsck.Violation.NO_ETAG);
      // check the child that there's no NO_ETAG violation
      checkNoViolationInPairs(file2, comparePairs,
          S3GuardFsck.Violation.NO_ETAG);
    } finally {
      cleanup(file1, file2, cwd);
    }
  }

  @Test
  public void testTombstonedInMsNotDeletedInS3() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touchGuardedAndWaitRaw(file);
      // set isDeleted flag in ms to true (tombstone item)
      final PathMetadata fileMeta = metadataStore.get(file);
      fileMeta.setIsDeleted(true);
      metadataStore.put(fileMeta);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3ToMs(cwd);

      assertComparePairsSize(comparePairs, 1);

      // check if the violation is there
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.TOMBSTONED_IN_MS_NOT_DELETED_IN_S3);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void checkDdbInternalConsistency() throws Exception {
    final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
    final DynamoDBMetadataStore ms =
        (DynamoDBMetadataStore) guardedFs.getMetadataStore();
    s3GuardFsck.checkDdbInternalConsistency(
        new Path("s3a://" + guardedFs.getBucket() + "/"));
  }

  @Test
  public void testDdbInternalNoLastUpdatedField() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      final S3AFileStatus s3AFileStatus = new S3AFileStatus(100, 100, file, 100,
          "test", "etag", "version");
      final PathMetadata pathMetadata = new PathMetadata(s3AFileStatus);
      pathMetadata.setLastUpdated(0);
      metadataStore.put(pathMetadata);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.checkDdbInternalConsistency(cwd);

      assertComparePairsSize(comparePairs, 1);

      // check if the violation is there
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.NO_LASTUPDATED_FIELD);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testDdbInternalOrphanEntry() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path parentDir = new Path(cwd, "directory");
    final Path file = new Path(parentDir, "file");
    try {
      final S3AFileStatus s3AFileStatus = new S3AFileStatus(100, 100, file, 100,
          "test", "etag", "version");
      final PathMetadata pathMetadata = new PathMetadata(s3AFileStatus);
      pathMetadata.setLastUpdated(1000);
      metadataStore.put(pathMetadata);
      metadataStore.forgetMetadata(parentDir);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.checkDdbInternalConsistency(cwd);

      // check if the violation is there
      assertComparePairsSize(comparePairs, 1);
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.ORPHAN_DDB_ENTRY);

      // fix the violation
      s3GuardFsck.fixViolations(
          comparePairs.stream().filter(cP -> cP.getViolations()
              .contains(S3GuardFsck.Violation.ORPHAN_DDB_ENTRY))
              .collect(Collectors.toList())
      );

      // assert that the violation is fixed
      final List<S3GuardFsck.ComparePair> fixedComparePairs =
          s3GuardFsck.checkDdbInternalConsistency(cwd);
      checkNoViolationInPairs(file, fixedComparePairs,
          S3GuardFsck.Violation.ORPHAN_DDB_ENTRY);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testDdbInternalParentIsAFile() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path parentDir = new Path(cwd, "directory");
    final Path file = new Path(parentDir, "file");
    try {
      final S3AFileStatus s3AFileStatus = new S3AFileStatus(100, 100, file, 100,
          "test", "etag", "version");
      final PathMetadata pathMetadata = new PathMetadata(s3AFileStatus);
      pathMetadata.setLastUpdated(1000);
      metadataStore.put(pathMetadata);

      final S3AFileStatus dirAsFile = MetadataStoreTestBase
          .basicFileStatus(parentDir, 1, false, 1);
      final PathMetadata dirAsFilePm = new PathMetadata(dirAsFile);
      dirAsFilePm.setLastUpdated(100);
      metadataStore.put(dirAsFilePm);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.checkDdbInternalConsistency(cwd);

      // check if the violation is there
      assertComparePairsSize(comparePairs, 1);
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.PARENT_IS_A_FILE);
    } finally {
      cleanup(file, cwd);
    }
  }

  @Test
  public void testDdbInternalParentTombstoned() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path parentDir = new Path(cwd, "directory");
    final Path file = new Path(parentDir, "file");
    try {
      final S3AFileStatus s3AFileStatus = new S3AFileStatus(100, 100, file, 100,
          "test", "etag", "version");
      final PathMetadata pathMetadata = new PathMetadata(s3AFileStatus);
      pathMetadata.setLastUpdated(1000);
      metadataStore.put(pathMetadata);
      metadataStore.delete(parentDir, null);

      final S3GuardFsck s3GuardFsck = new S3GuardFsck(rawFs, metadataStore);
      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.checkDdbInternalConsistency(cwd);

      // check if the violation is there
      assertComparePairsSize(comparePairs, 1);
      checkForViolationInPairs(file, comparePairs,
          S3GuardFsck.Violation.PARENT_TOMBSTONED);
    } finally {
      cleanup(file, cwd);
    }
  }

  protected void assertComparePairsSize(
      List<S3GuardFsck.ComparePair> comparePairs, int num) {
    Assertions.assertThat(comparePairs)
        .describedAs("Number of compare pairs")
        .hasSize(num);
  }

  private void touchGuardedAndWaitRaw(Path file) throws Exception {
    touchAndWait(guardedFs, rawFs, file);
  }

  private void touchRawAndWaitRaw(Path file) throws Exception {
    touchAndWait(rawFs, rawFs, file);
  }

  private void touchAndWait(FileSystem forTouch, FileSystem forWait, Path file)
      throws IOException {
    touch(forTouch, file);
    touch(forWait, file);
  }

  private void checkForViolationInPairs(Path file,
      List<S3GuardFsck.ComparePair> comparePairs,
      S3GuardFsck.Violation violation) {
    final S3GuardFsck.ComparePair childPair = comparePairs.stream()
        .filter(p -> p.getPath().equals(file))
        .findFirst().get();
    assertNotNull("The pair should not be null.", childPair);
    assertTrue("The pair must contain a violation.",
        childPair.containsViolation());
    Assertions.assertThat(childPair.getViolations())
        .describedAs("Violations in the pair")
        .contains(violation);
  }

  /**
   * Check that there is no violation in the pair provided.
   *
   * @param file the path to filter to in the comparePairs list.
   * @param comparePairs the list to validate.
   * @param violation the violation that should not be in the list.
   */
  private void checkNoViolationInPairs(Path file,
      List<S3GuardFsck.ComparePair> comparePairs,
      S3GuardFsck.Violation violation) {

    if (comparePairs.size() == 0) {
      LOG.info("Compare pairs is empty, so there's no violation. (As expected.)");
      return;
    }

    final S3GuardFsck.ComparePair comparePair = comparePairs.stream()
        .filter(p -> p.getPath().equals(file))
        .findFirst().get();
    assertNotNull("The pair should not be null.", comparePair);
    Assertions.assertThat(comparePair.getViolations())
        .describedAs("Violations in the pair")
        .doesNotContain(violation);
  }

  private void cleanup(Path... paths) {
    for (Path path : paths) {
      try {
        metadataStore.forgetMetadata(path);
        rawFs.delete(path, true);
      } catch (IOException e) {
        LOG.error("Error during cleanup.", e);
      }
    }
  }
}
