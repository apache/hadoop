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
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.s3a.MultipartTestUtils.assertNoUploadsAt;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.clearAnyUploads;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.countUploadsAt;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.createPartUpload;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getLandsatCSVFile;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.BucketInfo;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.E_BAD_STATE;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Uploads;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.exec;

/**
 * Test S3Guard Tool CLI commands.
 */
public class ITestS3GuardTool extends AbstractS3GuardToolTestBase {

  private static final String[] ABORT_FORCE_OPTIONS = new String[]{
      "-abort",
      "-force", "-verbose"};

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
            ITestS3GuardTool.this.getConfiguration()));
  }

  @Test
  public void testLandsatBucketRequireUnencrypted() throws Throwable {
    skipIfClientSideEncryption();
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
            ITestS3GuardTool.this.getConfiguration()));
  }

  @Test
  public void testStoreInfo() throws Throwable {
    S3GuardTool.BucketInfo cmd =
        toClose(new S3GuardTool.BucketInfo(getFileSystem().getConf()));
    String output = exec(cmd, cmd.getName(),
        "-" + BucketInfo.UNGUARDED_FLAG,
        getFileSystem().getUri().toString());
    LOG.info("Exec output=\n{}", output);
  }

  private final static String UPLOAD_PREFIX = "test-upload-prefix";
  private final static String UPLOAD_NAME = "test-upload";

  @Test
  public void testUploads() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path path = path(UPLOAD_PREFIX + "/" + UPLOAD_NAME);

    describe("Cleaning up any leftover uploads from previous runs.");
    final String key = fs.pathToKey(path);
    try {
      // 1. Make sure key doesn't already exist
      clearAnyUploads(fs, path);

      // 2. Confirm no uploads are listed via API
      assertNoUploadsAt(fs, path.getParent());

      // 3. Confirm no uploads are listed via CLI
      describe("Confirming CLI lists nothing.");
      assertNumUploads(path, 0);

      // 4. Create a upload part
      describe("Uploading single part.");
      createPartUpload(fs, key, 128, 1);

      assertEquals("Should be one upload", 1, countUploadsAt(fs, path));

      // 6. Confirm part exists via CLI, direct path and parent path
      describe("Confirming CLI lists one part");
      assertNumUploads(path, 1);
      assertNumUploads(path.getParent(), 1);
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
    final String key = fs.pathToKey(path);
    createPartUpload(fs, key, 128, 1);

    //try (AuditSpan span = fs.startOperation("multipart", key, null)) {
    try {

      // 3. Confirm it exists via API.. may want to wrap with
      // LambdaTestUtils.eventually() ?
      assertEquals("Should be one upload", 1, countUploadsAt(fs, path));

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
      LambdaTestUtils.eventually(5000, 1000, () -> {
        assertNumUploadsAge(path, 1, 1);
      });

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
  private void uploadCommandAssertCount(S3AFileSystem fs, String[] options, Path path,
      int numUploads, int ageSeconds)
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
