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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.StringUtils;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Tristate;

import static org.apache.hadoop.fs.s3a.MultipartTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getLandsatCSVFile;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.*;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.exec;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test S3Guard related CLI commands against a LocalMetadataStore.
 * Also responsible for testing the non s3guard-specific commands that, for
 * now, live under the s3guard CLI command.
 */
public class ITestS3GuardToolLocal extends AbstractS3GuardToolTestBase {

  private static final String LOCAL_METADATA = "local://metadata";
  private static final String[] ABORT_FORCE_OPTIONS = new String[] {"-abort",
      "-force", "-verbose"};

  @Override
  public void setup() throws Exception {
    super.setup();
    MetadataStore ms = getMetadataStore();
    Assume.assumeTrue("Test only applies when a local store is used for S3Guard;"
            + "Store is " + (ms == null ? "none" : ms.toString()),
        ms instanceof LocalMetadataStore);
  }

  @Test
  public void testImportCommand() throws Exception {
    S3AFileSystem fs = getFileSystem();
    MetadataStore ms = getMetadataStore();
    Path parent = path("test-import");
    fs.mkdirs(parent);
    Path dir = new Path(parent, "a");
    fs.mkdirs(dir);
    Path emptyDir = new Path(parent, "emptyDir");
    fs.mkdirs(emptyDir);
    for (int i = 0; i < 10; i++) {
      String child = String.format("file-%d", i);
      try (FSDataOutputStream out = fs.create(new Path(dir, child))) {
        out.write(1);
      }
    }

    S3GuardTool.Import cmd = toClose(new S3GuardTool.Import(fs.getConf()));
    try {
      cmd.setStore(ms);
      exec(cmd, "import", parent.toString());
    } finally {
      cmd.setStore(new NullMetadataStore());
    }

    DirListingMetadata children =
        ms.listChildren(dir);
    assertEquals("Unexpected number of paths imported", 10, children
        .getListing().size());
    assertEquals("Expected 2 items: empty directory and a parent directory", 2,
        ms.listChildren(parent).getListing().size());
    assertTrue(children.isAuthoritative());
  }

  @Test
  public void testImportCommandRepairsETagAndVersionId() throws Exception {
    S3AFileSystem fs = getFileSystem();
    MetadataStore ms = getMetadataStore();
    Path path = path("test-version-metadata");
    try (FSDataOutputStream out = fs.create(path)) {
      out.write(1);
    }
    S3AFileStatus originalStatus = (S3AFileStatus) fs.getFileStatus(path);

    // put in bogus ETag and versionId
    S3AFileStatus bogusStatus = S3AFileStatus.fromFileStatus(originalStatus,
        Tristate.FALSE, "bogusETag", "bogusVersionId");
    ms.put(new PathMetadata(bogusStatus));

    // sanity check that bogus status is actually persisted
    S3AFileStatus retrievedBogusStatus = (S3AFileStatus) fs.getFileStatus(path);
    assertEquals("bogus ETag was not persisted",
        "bogusETag", retrievedBogusStatus.getETag());
    assertEquals("bogus versionId was not persisted",
        "bogusVersionId", retrievedBogusStatus.getVersionId());

    // execute the import
    S3GuardTool.Import cmd = toClose(new S3GuardTool.Import(fs.getConf()));
    cmd.setStore(ms);
    try {
      exec(cmd, "import", path.toString());
    } finally {
      cmd.setStore(new NullMetadataStore());
    }

    // make sure ETag and versionId were corrected
    S3AFileStatus updatedStatus = (S3AFileStatus) fs.getFileStatus(path);
    assertEquals("ETag was not corrected",
        originalStatus.getETag(), updatedStatus.getETag());
    assertEquals("VersionId was not corrected",
        originalStatus.getVersionId(), updatedStatus.getVersionId());
  }

  @Test
  public void testDestroyBucketExistsButNoTable() throws Throwable {
    run(Destroy.NAME,
        "-meta", LOCAL_METADATA,
        getLandsatCSVFile(getConfiguration()));
  }

  @Test
  public void testImportNoFilesystem() throws Throwable {
    final Import importer = toClose(new S3GuardTool.Import(getConfiguration()));
    importer.setStore(getMetadataStore());
    try {
      intercept(IOException.class,
          () -> importer.run(
              new String[]{
                  "import",
                  "-meta", LOCAL_METADATA,
                  S3A_THIS_BUCKET_DOES_NOT_EXIST
              }));
    } finally {
      importer.setStore(new NullMetadataStore());
    }
  }

  @Test
  public void testInfoBucketAndRegionNoFS() throws Throwable {
    intercept(FileNotFoundException.class,
        () -> run(BucketInfo.NAME, "-meta",
            LOCAL_METADATA, "-region",
            "any-region", S3A_THIS_BUCKET_DOES_NOT_EXIST));
  }

  @Test
  public void testInitNegativeRead() throws Throwable {
    runToFailure(INVALID_ARGUMENT,
        Init.NAME, "-meta", LOCAL_METADATA, "-region",
        "eu-west-1",
        READ_FLAG, "-10");
  }

  @Test
  public void testInit() throws Throwable {
    run(Init.NAME,
        "-meta", LOCAL_METADATA,
        "-region", "us-west-1");
  }

  @Test
  public void testInitTwice() throws Throwable {
    run(Init.NAME,
        "-meta", LOCAL_METADATA,
        "-region", "us-west-1");
    run(Init.NAME,
        "-meta", LOCAL_METADATA,
        "-region", "us-west-1");
  }

  @Test
  public void testLandsatBucketUnguarded() throws Throwable {
    run(BucketInfo.NAME,
        "-" + BucketInfo.UNGUARDED_FLAG,
        getLandsatCSVFile(getConfiguration()));
  }

  @Test
  public void testLandsatBucketRequireGuarded() throws Throwable {
    runToFailure(E_BAD_STATE,
        BucketInfo.NAME,
        "-" + BucketInfo.GUARDED_FLAG,
        getLandsatCSVFile(
            ITestS3GuardToolLocal.this.getConfiguration()));
  }

  @Test
  public void testLandsatBucketRequireUnencrypted() throws Throwable {
    run(BucketInfo.NAME,
        "-" + BucketInfo.ENCRYPTION_FLAG, "none",
        getLandsatCSVFile(getConfiguration()));
  }

  @Test
  public void testLandsatBucketRequireEncrypted() throws Throwable {
    runToFailure(E_BAD_STATE,
        BucketInfo.NAME,
        "-" + BucketInfo.ENCRYPTION_FLAG,
        "AES256", getLandsatCSVFile(
            ITestS3GuardToolLocal.this.getConfiguration()));
  }

  @Test
  public void testStoreInfo() throws Throwable {
    S3GuardTool.BucketInfo cmd =
        toClose(new S3GuardTool.BucketInfo(getFileSystem().getConf()));
    cmd.setStore(getMetadataStore());
    try {
      String output = exec(cmd, cmd.getName(),
          "-" + BucketInfo.GUARDED_FLAG,
          getFileSystem().getUri().toString());
      LOG.info("Exec output=\n{}", output);
    } finally {
      cmd.setStore(new NullMetadataStore());
    }
  }

  @Test
  public void testSetCapacity() throws Throwable {
    S3GuardTool cmd = toClose(
        new S3GuardTool.SetCapacity(getFileSystem().getConf()));
    cmd.setStore(getMetadataStore());
    try {
      String output = exec(cmd, cmd.getName(),
          "-" + READ_FLAG, "100",
          "-" + WRITE_FLAG, "100",
          getFileSystem().getUri().toString());
      LOG.info("Exec output=\n{}", output);
    } finally {
      cmd.setStore(new NullMetadataStore());
    }
  }

  private final static String UPLOAD_PREFIX = "test-upload-prefix";
  private final static String UPLOAD_NAME = "test-upload";

  @Test
  public void testUploads() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path path = path(UPLOAD_PREFIX + "/" + UPLOAD_NAME);

    describe("Cleaning up any leftover uploads from previous runs.");
    // 1. Make sure key doesn't already exist
    clearAnyUploads(fs, path);

    // 2. Confirm no uploads are listed via API
    assertNoUploadsAt(fs, path.getParent());

    // 3. Confirm no uploads are listed via CLI
    describe("Confirming CLI lists nothing.");
    assertNumUploads(path, 0);

    // 4. Create a upload part
    describe("Uploading single part.");
    createPartUpload(fs, fs.pathToKey(path), 128, 1);

    try {
      // 5. Confirm it exists via API..
      LambdaTestUtils.eventually(5000, /* 5 seconds until failure */
          1000, /* one second retry interval */
          () -> {
            assertEquals("Should be one upload", 1, countUploadsAt(fs, path));
          });

      // 6. Confirm part exists via CLI, direct path and parent path
      describe("Confirming CLI lists one part");
      LambdaTestUtils.eventually(5000, 1000,
          () -> { assertNumUploads(path, 1); });
      LambdaTestUtils.eventually(5000, 1000,
          () -> { assertNumUploads(path.getParent(), 1); });

      // 7. Use CLI to delete part, assert it worked
      describe("Deleting part via CLI");
      assertNumDeleted(fs, path, 1);

      // 8. Confirm deletion via API
      describe("Confirming deletion via API");
      assertEquals("Should be no uploads", 0, countUploadsAt(fs, path));

      // 9. Confirm no uploads are listed via CLI
      describe("Confirming CLI lists nothing.");
      assertNumUploads(path, 0);

    } catch (Throwable t) {
      // Clean up on intermediate failure
      clearAnyUploads(fs, path);
      throw t;
    }
  }

  @Test
  public void testUploadListByAge() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path path = path(UPLOAD_PREFIX + "/" + UPLOAD_NAME);

    describe("Cleaning up any leftover uploads from previous runs.");
    // 1. Make sure key doesn't already exist
    clearAnyUploads(fs, path);

    // 2. Create a upload part
    describe("Uploading single part.");
    createPartUpload(fs, fs.pathToKey(path), 128, 1);

    try {
      // 3. Confirm it exists via API.. may want to wrap with
      // LambdaTestUtils.eventually() ?
      LambdaTestUtils.eventually(5000, 1000,
          () -> {
            assertEquals("Should be one upload", 1, countUploadsAt(fs, path));
          });

      // 4. Confirm part does appear in listing with long age filter
      describe("Confirming CLI older age doesn't list");
      assertNumUploadsAge(path, 0, 600);

      // 5. Confirm part does not get deleted with long age filter
      describe("Confirming CLI older age doesn't delete");
      uploadCommandAssertCount(fs, ABORT_FORCE_OPTIONS, path, 0,
          600);

      // 6. Wait a second and then assert the part is in listing of things at
      // least a second old
      describe("Sleeping 1 second then confirming upload still there");
      Thread.sleep(1000);
      LambdaTestUtils.eventually(5000, 1000,
          () -> { assertNumUploadsAge(path, 1, 1); });

      // 7. Assert deletion works when age filter matches
      describe("Doing aged deletion");
      uploadCommandAssertCount(fs, ABORT_FORCE_OPTIONS, path, 1, 1);
      describe("Confirming age deletion happened");
      assertEquals("Should be no uploads", 0, countUploadsAt(fs, path));
    } catch (Throwable t) {
      // Clean up on intermediate failure
      clearAnyUploads(fs, path);
      throw t;
    }
  }

  @Test
  public void testUploadNegativeExpect() throws Throwable {
    runToFailure(E_BAD_STATE, Uploads.NAME, "-expect", "1",
        path("/we/are/almost/postive/this/doesnt/exist/fhfsadfoijew")
            .toString());
  }

  private void assertNumUploads(Path path, int numUploads) throws Exception {
    assertNumUploadsAge(path, numUploads, 0);
  }

  private void assertNumUploadsAge(Path path, int numUploads, int ageSeconds)
      throws Exception {
    if (ageSeconds > 0) {
      run(Uploads.NAME, "-expect", String.valueOf(numUploads), "-seconds",
          String.valueOf(ageSeconds), path.toString());
    } else {
      run(Uploads.NAME, "-expect", String.valueOf(numUploads), path.toString());
    }
  }

  private void assertNumDeleted(S3AFileSystem fs, Path path, int numDeleted)
      throws Exception {
    uploadCommandAssertCount(fs, ABORT_FORCE_OPTIONS, path,
        numDeleted, 0);
  }

  /**
   * Run uploads cli command and assert the reported count (listed or
   * deleted) matches.
   * @param fs  S3AFileSystem
   * @param options main command options
   * @param path path of part(s)
   * @param numUploads expected number of listed/deleted parts
   * @param ageSeconds optional seconds of age to specify to CLI, or zero to
   *                   search all parts
   * @throws Exception on failure
   */
  private void uploadCommandAssertCount(S3AFileSystem fs, String options[],
      Path path, int numUploads, int ageSeconds)
      throws Exception {
    List<String> allOptions = new ArrayList<>();
    List<String> output = new ArrayList<>();
    S3GuardTool.Uploads cmd = new S3GuardTool.Uploads(fs.getConf());
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    allOptions.add(cmd.getName());
    allOptions.addAll(Arrays.asList(options));
    if (ageSeconds > 0) {
      allOptions.add("-" + Uploads.SECONDS_FLAG);
      allOptions.add(String.valueOf(ageSeconds));
    }
    allOptions.add(path.toString());
    exec(0, "", cmd, buf, allOptions.toArray(new String[0]));

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new ByteArrayInputStream(buf.toByteArray())))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] fields = line.split("\\s");
        if (fields.length == 4 && fields[0].equals(Uploads.TOTAL)) {
          int parsedUploads = Integer.parseInt(fields[1]);
          LOG.debug("Matched CLI output: {} {} {} {}",
              fields[0], fields[1], fields[2], fields[3]);
          assertEquals("Unexpected number of uploads", numUploads,
              parsedUploads);
          return;
        }
        LOG.debug("Not matched: {}", line);
        output.add(line);
      }
    }
    fail("Command output did not match: \n" + StringUtils.join("\n", output));
  }
}
