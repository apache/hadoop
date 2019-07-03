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

import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.awaitFileStatus;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class ITestS3GuardFsck extends AbstractS3ATestBase {

  private S3AFileSystem guardedFs;
  private S3AFileSystem rawFS;

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
    rawFS = createUnguardedFS();
    assertFalse("Raw FS still has S3Guard " + rawFS,
        rawFS.hasMetadataStore());
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
        S3_METADATA_STORE_IMPL);
    removeBaseAndBucketOverrides(uri.getHost(), config,
        METADATASTORE_AUTHORITATIVE);
    S3AFileSystem fs2 = new S3AFileSystem();
    fs2.initialize(uri, config);
    return fs2;
  }

  @Test
  public void testIDetectNoMetadataEntry() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      touch(rawFS, file);
      awaitFileStatus(rawFS, file);

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two.", 2,
          comparePairs.size());
      final S3GuardFsck.ComparePair pair = comparePairs.get(0);
      assumeTrue("The pair must contain a violation.", pair.containsViolation());
      assertEquals("The pair must contain only one violation", 1,
          pair.getViolations().size());

      final S3GuardFsck.Violation violation =
          pair.getViolations().iterator().next();
      assertEquals("The violation should be that there is no violation entry.",
          violation, S3GuardFsck.Violation.NO_METADATA_ENTRY);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testIDetectNoParentEntry() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touch(guardedFs, file);
      awaitFileStatus(guardedFs, file);

      // delete the parent from the MS
      metadataStore.forgetMetadata(cwd);

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two. The cwd (parent) and the "
              + "child.", 2, comparePairs.size());

      // check the parent that it does not exist
      final S3GuardFsck.ComparePair cwdPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(cwd))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", cwdPair);
      assumeTrue("The cwdPair must contain a violation.", cwdPair.containsViolation());
      Assertions.assertThat(cwdPair.getViolations())
          .describedAs("Violations in the cwdPair")
          .contains(S3GuardFsck.Violation.NO_METADATA_ENTRY);

      // check the child that there's no parent entry.
      final S3GuardFsck.ComparePair childPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", childPair);
      assumeTrue("The childPair must contain a violation.", childPair.containsViolation());
      Assertions.assertThat(childPair.getViolations())
          .describedAs("Violations in the childPair")
          .contains(S3GuardFsck.Violation.NO_PARENT_ENTRY);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testIDetectParentIsAFile() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touch(guardedFs, file);
      awaitFileStatus(guardedFs, file);

      // modify the cwd metadata and set that it's not a directory
      final S3AFileStatus newParentFile = MetadataStoreTestBase
          .basicFileStatus(cwd, 1, false, 1);
      metadataStore.put(new PathMetadata(newParentFile));

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two. The cwd (parent) and the "
              + "child.", 2, comparePairs.size());

      // check the parent that it does not exist
      final S3GuardFsck.ComparePair cwdPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(cwd))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", cwdPair);
      assumeTrue("The cwdPair must contain a violation.", cwdPair.containsViolation());
      Assertions.assertThat(cwdPair.getViolations())
          .describedAs("Violations in the cwdPair")
          .contains(S3GuardFsck.Violation.DIR_IN_S3_FILE_IN_MS);

      // check the child that the parent is a file.
      final S3GuardFsck.ComparePair childPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", childPair);
      assumeTrue("The childPair must contain a violation.", childPair.containsViolation());
      Assertions.assertThat(childPair.getViolations())
          .describedAs("Violations in the childPair")
          .contains(S3GuardFsck.Violation.PARENT_IS_A_FILE);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testIDetectParentTombstoned() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touch(guardedFs, file);
      awaitFileStatus(guardedFs, file);

      // modify the parent metadata and set that it's not a directory
      final PathMetadata cwdPmd = metadataStore.get(cwd);
      cwdPmd.setIsDeleted(true);
      metadataStore.put(cwdPmd);

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two. The cwd (parent) and the "
              + "child.", 2, comparePairs.size());

      // check the child that the parent is tombstoned
      final S3GuardFsck.ComparePair childPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", childPair);
      assumeTrue("The childPair must contain a violation.", childPair.containsViolation());
      Assertions.assertThat(childPair.getViolations())
          .describedAs("Violations in the childPair")
          .contains(S3GuardFsck.Violation.PARENT_TOMBSTONED);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
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

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pair should be one. Only the cwd.", 1,
          comparePairs.size());

      // check the child that the dir in s3 is a file in the ms
      final S3GuardFsck.ComparePair cwdPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(cwd))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", cwdPair);
      assumeTrue("The cwdPair must contain a violation.", cwdPair.containsViolation());
      Assertions.assertThat(cwdPair.getViolations())
          .describedAs("Violations in the cwdPair")
          .contains(S3GuardFsck.Violation.DIR_IN_S3_FILE_IN_MS);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testIDetectLengthMismatch() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touch(guardedFs, file);
      awaitFileStatus(guardedFs, file);

      // modify the file metadata so the length will not match
      final S3AFileStatus newFile = MetadataStoreTestBase
          .basicFileStatus(file, 9999, false, 1);
      metadataStore.put(new PathMetadata(newFile));

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two. The cwd (parent) and the "
              + "child.", 2, comparePairs.size());

      // check the parent that it does not contain LENGTH_MISMATCH
      final S3GuardFsck.ComparePair cwdPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(cwd))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", cwdPair);
      assumeTrue("The cwdPair must contain a violation.", cwdPair.containsViolation());
      Assertions.assertThat(cwdPair.getViolations())
          .describedAs("Violations in the cwdPair")
          .doesNotContain(S3GuardFsck.Violation.LENGTH_MISMATCH);

      // check the child that there's a LENGTH_MISMATCH
      final S3GuardFsck.ComparePair childPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", childPair);
      assumeTrue("The childPair must contain a violation.", childPair.containsViolation());
      Assertions.assertThat(childPair.getViolations())
          .describedAs("Violations in the childPair")
          .contains(S3GuardFsck.Violation.LENGTH_MISMATCH);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testIDetectModTimeMismatch() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touch(guardedFs, file);
      awaitFileStatus(guardedFs, file);

      // modify the file metadata so the length will not match
      final S3AFileStatus newFileStatus = MetadataStoreTestBase
          .basicFileStatus(file, 0, false, 1);
      metadataStore.put(new PathMetadata(newFileStatus));

      // modify the parent meta entry so the MOD_TIME will surely be up to date
      final FileStatus oldCwdFileStatus = rawFS.getFileStatus(cwd);
      final S3AFileStatus newCwdFileStatus = MetadataStoreTestBase
          .basicFileStatus(file, 0, true,
              oldCwdFileStatus.getModificationTime());
      metadataStore.put(new PathMetadata(newCwdFileStatus));

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two. The cwd (parent) and the "
              + "child.", 2, comparePairs.size());

      // check the parent that it does not contain MOD_TIME_MISMATCH
      final S3GuardFsck.ComparePair cwdPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(cwd))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", cwdPair);
      assumeTrue("The cwdPair must contain a violation.", cwdPair.containsViolation());
      Assertions.assertThat(cwdPair.getViolations())
          .describedAs("Violations in the cwdPair")
          .doesNotContain(S3GuardFsck.Violation.MOD_TIME_MISMATCH);

      // check the child that there's a MOD_TIME_MISMATCH
      final S3GuardFsck.ComparePair childPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", childPair);
      assumeTrue("The childPair must contain a violation.", childPair.containsViolation());
      Assertions.assertThat(childPair.getViolations())
          .describedAs("Violations in the childPair")
          .contains(S3GuardFsck.Violation.MOD_TIME_MISMATCH);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testIBlockSizeMismatch() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touch(guardedFs, file);
      awaitFileStatus(guardedFs, file);

      // modify the file metadata so the length will not match
      final S3AFileStatus newFileStatus = MetadataStoreTestBase
          .basicFileStatus(1, false, 999, 1, file);
      metadataStore.put(new PathMetadata(newFileStatus));

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two. The cwd (parent) and the "
          + "child.", 2, comparePairs.size());

      // check the parent that it does not contain BLOCKSIZE_MISMATCH
      final S3GuardFsck.ComparePair cwdPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(cwd))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", cwdPair);
      assumeTrue("The childPair must contain a violation.", cwdPair.containsViolation());
      Assertions.assertThat(cwdPair.getViolations())
          .describedAs("Violations in the cwdPair")
          .doesNotContain(S3GuardFsck.Violation.BLOCKSIZE_MISMATCH);

      // check the child that there's a BLOCKSIZE_MISMATCH
      final S3GuardFsck.ComparePair childPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", childPair);
      assumeTrue("The childPair must contain a violation.", childPair.containsViolation());
      Assertions.assertThat(childPair.getViolations())
          .describedAs("Violations in the childPair")
          .contains(S3GuardFsck.Violation.BLOCKSIZE_MISMATCH);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testIOwnerMismatch() throws Exception {
    skip("We don't store the owner in dynamo, so there's no test for this.");
  }

  @Test
  public void testIVersionIdMismatch() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touch(guardedFs, file);
      awaitFileStatus(guardedFs, file);

      // modify the file metadata so the versionId will not match
      final S3AFileStatus newFileStatus = new S3AFileStatus(1, 1, file, 1, ""
          , "etag", "versionId");
      metadataStore.put(new PathMetadata(newFileStatus));

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two. The cwd (parent) and the "
          + "child.", 2, comparePairs.size());

      // check the parent that it does not contain BLOCKSIZE_MISMATCH
      final S3GuardFsck.ComparePair cwdPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(cwd))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", cwdPair);
      assumeTrue("The childPair must contain a violation.", cwdPair.containsViolation());
      Assertions.assertThat(cwdPair.getViolations())
          .describedAs("Violations in the cwdPair")
          .doesNotContain(S3GuardFsck.Violation.VERSIONID_MISMATCH);

      // check the child that there's a BLOCKSIZE_MISMATCH
      final S3GuardFsck.ComparePair childPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", childPair);
      assumeTrue("The childPair must contain a violation.", childPair.containsViolation());
      Assertions.assertThat(childPair.getViolations())
          .describedAs("Violations in the childPair")
          .contains(S3GuardFsck.Violation.VERSIONID_MISMATCH);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testIEtagMismatch() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file = new Path(cwd, "file");
    try {
      // create a file with guarded fs
      touch(guardedFs, file);
      awaitFileStatus(guardedFs, file);

      // modify the file metadata so the etag will not match
      final S3AFileStatus newFileStatus = new S3AFileStatus(1, 1, file, 1, ""
          , "etag", "versionId");
      metadataStore.put(new PathMetadata(newFileStatus));

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be two. The cwd (parent) and the "
          + "child.", 2, comparePairs.size());

      // check the parent that it does not contain BLOCKSIZE_MISMATCH
      final S3GuardFsck.ComparePair cwdPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(cwd))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", cwdPair);
      assumeTrue("The childPair must contain a violation.", cwdPair.containsViolation());
      Assertions.assertThat(cwdPair.getViolations())
          .describedAs("Violations in the cwdPair")
          .doesNotContain(S3GuardFsck.Violation.ETAG_MISMATCH);

      // check the child that there's a BLOCKSIZE_MISMATCH
      final S3GuardFsck.ComparePair childPair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", childPair);
      assumeTrue("The childPair must contain a violation.", childPair.containsViolation());
      Assertions.assertThat(childPair.getViolations())
          .describedAs("Violations in the childPair")
          .contains(S3GuardFsck.Violation.ETAG_MISMATCH);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testINoEtag() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file1 = new Path(cwd, "file1");
    final Path file2 = new Path(cwd, "file2");
    try {
      // create a file1 with guarded fs
      touch(guardedFs, file1);
      touch(guardedFs, file2);
      awaitFileStatus(guardedFs, file1);
      awaitFileStatus(guardedFs, file2);

      // modify the file1 metadata so there's no etag
      final S3AFileStatus newFile1Status =
          new S3AFileStatus(1, 1, file1, 1, "", null, "versionId");
      final S3AFileStatus newFile2Status =
          new S3AFileStatus(1, 1, file2, 1, "", "etag", "versionId");

      metadataStore.put(new PathMetadata(newFile1Status));
      metadataStore.put(new PathMetadata(newFile2Status));

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be 3. The cwd (parent) and the "
          + "2 children.", 3, comparePairs.size());

      // check file 1 that there's NO_ETAG
      final S3GuardFsck.ComparePair file1Pair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file1))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", file1Pair);
      assumeTrue("The file1Pair must contain a violation.",
          file1Pair.containsViolation());
      Assertions.assertThat(file1Pair.getViolations())
          .describedAs("Violations in the file1Pair")
          .contains(S3GuardFsck.Violation.NO_ETAG);

      // check the child that there's no NO_ETAG violation
      final S3GuardFsck.ComparePair file2Pair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file2))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", file2Pair);
      assumeTrue("The file2Pair must contain a violation.",
          file2Pair.containsViolation());
      Assertions.assertThat(file2Pair.getViolations())
          .describedAs("Violations in the file2Pair")
          .doesNotContain(S3GuardFsck.Violation.NO_ETAG);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file1);
      metadataStore.forgetMetadata(file2);
      metadataStore.forgetMetadata(cwd);
    }
  }

  @Test
  public void testINoVersionId() throws Exception {
    final Path cwd = path("/" + getMethodName() + "-" + UUID.randomUUID());
    final Path file1 = new Path(cwd, "file1");
    final Path file2 = new Path(cwd, "file2");
    try {
      // create a file1 with guarded fs
      touch(guardedFs, file1);
      touch(guardedFs, file2);
      awaitFileStatus(guardedFs, file1);
      awaitFileStatus(guardedFs, file2);

      // modify the file1 metadata so there's no versionId
      final S3AFileStatus newFile1Status =
          new S3AFileStatus(1, 1, file1, 1, "", "etag", null);
      // force add versionID to the second file
      final S3AFileStatus newFile2Status =
          new S3AFileStatus(1, 1, file2, 1, "", "etag", "notnull");

      metadataStore.put(new PathMetadata(newFile1Status));
      metadataStore.put(new PathMetadata(newFile2Status));

      final S3GuardFsck s3GuardFsck =
          new S3GuardFsck(rawFS, metadataStore, false);

      final List<S3GuardFsck.ComparePair> comparePairs =
          s3GuardFsck.compareS3toMs(cwd);

      assertEquals("Number of pairs should be 3. The cwd (parent) and the "
          + "2 children.", 3, comparePairs.size());

      // check file 1 that there's NO_ETAG
      final S3GuardFsck.ComparePair file1Pair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file1))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", file1Pair);
      assumeTrue("The file1Pair must contain a violation.",
          file1Pair.containsViolation());
      Assertions.assertThat(file1Pair.getViolations())
          .describedAs("Violations in the file1Pair")
          .contains(S3GuardFsck.Violation.NO_VERSIONID);

      // check file 2 that there's no NO_ETAG violation
      final S3GuardFsck.ComparePair file2Pair = comparePairs.stream()
          .filter(p -> p.getS3FileStatus().getPath().equals(file2))
          .findFirst().get();
      assumeNotNull("The pair should not be null.", file2Pair);
      assumeTrue("The file2Pair must contain a violation.",
          file2Pair.containsViolation());
      Assertions.assertThat(file2Pair.getViolations())
          .describedAs("Violations in the file2Pair")
          .doesNotContain(S3GuardFsck.Violation.NO_VERSIONID);
    } finally {
      // delete the working directory with all of its contents
      rawFS.delete(cwd, true);
      metadataStore.forgetMetadata(file1);
      metadataStore.forgetMetadata(file2);
      metadataStore.forgetMetadata(cwd);
    }
  }
}