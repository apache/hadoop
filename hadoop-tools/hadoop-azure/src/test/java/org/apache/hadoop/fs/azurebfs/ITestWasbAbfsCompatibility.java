/**
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
package org.apache.hadoop.fs.azurebfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDeleted;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test compatibility between ABFS client and WASB client.
 */
public class ITestWasbAbfsCompatibility extends AbstractAbfsIntegrationTest {
  private static final String WASB_TEST_CONTEXT = "wasb test file";
  private static final String ABFS_TEST_CONTEXT = "abfs test file";
  private static final String TEST_CONTEXT = "THIS IS FOR TEST";
  private static final String TEST_CONTEXT1 = "THIS IS FOR TEST1";
  private static final byte[] ATTRIBUTE_VALUE_1 = "one".getBytes(
      StandardCharsets.UTF_8);
  private static final byte[] ATTRIBUTE_VALUE_2 = "two".getBytes(
      StandardCharsets.UTF_8);
  private static final String ATTRIBUTE_NAME_1 = "user_someAttribute";
  private static final String ATTRIBUTE_NAME_2 = "user_someAttribute1";
  private static final EnumSet<XAttrSetFlag> CREATE_FLAG = EnumSet.of(
      XAttrSetFlag.CREATE);
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestWasbAbfsCompatibility.class);

  public ITestWasbAbfsCompatibility() throws Exception {
    assumeThat(isIPAddress()).as("Emulator is not supported").isFalse();
  }

  @Test
  public void testListFileStatus() throws Exception {
    // crate file using abfs
    AzureBlobFileSystem fs = getFileSystem();
    // test only valid for non-namespace enabled account
    assumeThat(getIsNamespaceEnabled(fs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();

    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFiles = path("/testfiles");
    Path path1 = new Path(testFiles + "/~12/!008/3/abFsTestfile");
    try (FSDataOutputStream abfsStream = fs.create(path1, true)) {
      abfsStream.write(ABFS_TEST_CONTEXT.getBytes());
      abfsStream.flush();
      abfsStream.hsync();
    }

    // create file using wasb
    Path path2 = new Path(testFiles + "/~12/!008/3/nativeFsTestfile");
    LOG.info("{}", wasb.getUri());
    try (FSDataOutputStream nativeFsStream = wasb.create(path2, true)) {
      nativeFsStream.write(WASB_TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // list file using abfs and wasb
    FileStatus[] abfsFileStatus = fs.listStatus(
        new Path(testFiles + "/~12/!008/3/"));
    FileStatus[] nativeFsFileStatus = wasb.listStatus(
        new Path(testFiles + "/~12/!008/3/"));

    assertEquals(2, abfsFileStatus.length);
    assertEquals(2, nativeFsFileStatus.length);
  }

  @Test
  public void testReadFile() throws Exception {
    boolean[] createFileWithAbfs = new boolean[]{false, true, false, true};
    boolean[] readFileWithAbfs = new boolean[]{false, true, true, false};

    AzureBlobFileSystem abfs = getFileSystem();
    // test only valid for non-namespace enabled account
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();

    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    for (int i = 0; i < 4; i++) {
      Path path = new Path(testFile + "/~12/!008/testfile" + i);
      final FileSystem createFs = createFileWithAbfs[i] ? abfs : wasb;
      // Read
      final FileSystem readFs = readFileWithAbfs[i] ? abfs : wasb;
      // Write
      try (FSDataOutputStream nativeFsStream = createFs.create(path, true)) {
        nativeFsStream.write(TEST_CONTEXT.getBytes());
        nativeFsStream.flush();
        nativeFsStream.hsync();
      }

      // Check file status
      ContractTestUtils.assertIsFile(createFs, path);

      try (BufferedReader br = new BufferedReader(
          new InputStreamReader(readFs.open(path)))) {
        String line = br.readLine();
        assertEquals(TEST_CONTEXT, line, "Wrong text from " + readFs);
      }

      // Remove file
      assertDeleted(readFs, path, true);
    }
  }

  /**
   * Flow: Create and write a file using WASB, then read and append to it using ABFS. Finally, delete the file via ABFS after verifying content consistency.
   * Expected: WASB successfully creates the file and writes content. ABFS reads, appends, and deletes the file without data loss or errors.
   */
  @Test
  public void testwriteFile() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(wasb, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    try (FSDataOutputStream abfsOutputStream = abfs.append(path)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }
    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Flow: Create and write a file using ABFS, append to the file using WASB, then write again using ABFS.
   * Expected: File is created and written correctly by ABFS, appended by WASB, and final ABFS write reflects all updates without errors.
   */

  @Test
  public void testwriteFile1() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    // Write
    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (FSDataOutputStream nativeFsStream = wasb.append(path)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    try (FSDataOutputStream nativeFsStream = abfs.append(path)) {
      nativeFsStream.write(TEST_CONTEXT1.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Flow: Create the file using AzCopy, then append to the file using ABFS.
   * Expected: ABFS append succeeds and final file reflects both AzCopy and appended data correctly.
   */
  @Test
  public void testazcopywasbcompatibility() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    createAzCopyFile(path);

    try (FSDataOutputStream nativeFsStream = abfs.append(path)) {
      nativeFsStream.write(TEST_CONTEXT1.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // Remove file
    assertDeleted(abfs, path, true);
  }


  @Test
  public void testDir() throws Exception {
    boolean[] createDirWithAbfs = new boolean[]{false, true, false, true};
    boolean[] readDirWithAbfs = new boolean[]{false, true, true, false};

    AzureBlobFileSystem abfs = getFileSystem();
    // test only valid for non-namespace enabled account
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();

    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testDir = path("/testDir");
    for (int i = 0; i < 4; i++) {
      Path path = new Path(testDir + "/t" + i);
      //create
      final FileSystem createFs = createDirWithAbfs[i] ? abfs : wasb;
      assertTrue(createFs.mkdirs(path));
      //check
      assertPathExists(createFs, "Created dir not found with " + createFs,
          path);
      //read
      final FileSystem readFs = readDirWithAbfs[i] ? abfs : wasb;
      assertPathExists(readFs, "Created dir not found with " + readFs,
          path);
      assertIsDirectory(readFs, path);
      assertDeleted(readFs, path, true);
    }
  }


  @Test
  public void testUrlConversion() {
    String abfsUrl
        = "abfs://abcde-1111-1111-1111-1111@xxxx.dfs.xxx.xxx.xxxx.xxxx";
    String wabsUrl
        = "wasb://abcde-1111-1111-1111-1111@xxxx.blob.xxx.xxx.xxxx.xxxx";
    assertEquals(abfsUrl, wasbUrlToAbfsUrl(wabsUrl));
    assertEquals(wabsUrl, abfsUrlToWasbUrl(abfsUrl, false));
  }

  @Test
  public void testSetWorkingDirectory() throws Exception {
    //create folders
    AzureBlobFileSystem abfs = getFileSystem();
    // test only valid for non-namespace enabled account
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();

    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path d1 = path("/d1");
    Path d1d4 = new Path(d1 + "/d2/d3/d4");
    assertMkdirs(abfs, d1d4);

    //set working directory to path1
    Path path1 = new Path(d1 + "/d2");
    wasb.setWorkingDirectory(path1);
    abfs.setWorkingDirectory(path1);
    assertEquals(path1, wasb.getWorkingDirectory());
    assertEquals(path1, abfs.getWorkingDirectory());

    //set working directory to path2
    Path path2 = new Path("d3/d4");
    wasb.setWorkingDirectory(path2);
    abfs.setWorkingDirectory(path2);

    Path path3 = d1d4;
    assertEquals(path3, wasb.getWorkingDirectory());
    assertEquals(path3, abfs.getWorkingDirectory());
  }

  // Scenario wise testing

  /**
   * Scenario 1: Create and write a file using WASB, then read the file using ABFS.
   * Expected Outcome: ABFS should correctly read the content written by WASB.
   */
  @Test
  public void testScenario1() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // Check file status
    ContractTestUtils.assertIsFile(wasb, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 2: Create and write a file using WASB, read it using ABFS, then write to the same file using ABFS.
   * Expected Outcome: ABFS should read the WASB-written content and successfully write new content to the same file.
   */
  @Test
  public void testScenario2() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // Check file status
    ContractTestUtils.assertIsFile(wasb, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }

    // Write
    try (FSDataOutputStream abfsOutputStream = abfs.append(path)) {
      abfsOutputStream.write(TEST_CONTEXT1.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 3: Create and write a file using ABFS, then read it using WASB.
   * Expected Outcome: WASB should be able to read the content written by ABFS without any data mismatch or error.
   */
  @Test
  public void testScenario3() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream abfsOutputStream = abfs.create(path, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(wasb.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + wasb);
    }
    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 4: Create a file using WASB, write to it using ABFS, and then write again using WASB.
   * Expected Outcome: All writes should succeed and the final content should reflect changes from both ABFS and WASB.
   */
  @Test
  public void testScenario4() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    wasb.create(path, true);
    try (FSDataOutputStream abfsOutputStream = abfs.append(path)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    try (FSDataOutputStream nativeFsStream = wasb.append(path)) {
      nativeFsStream.write(TEST_CONTEXT1.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);
    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 5: Create a file using ABFS, write to it using WASB, and read it back using ABFS with checksum validation disabled.
   * Expected Outcome: The read operation should succeed and reflect the data written via WASB despite checksum validation being off.
   */
  @Test
  public void testScenario5() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, false);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    abfs.create(path, true);
    try (FSDataOutputStream nativeFsStream = wasb.append(path)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 6: Create a file using ABFS, write to it using WASB, and read it via ABFS with checksum validation enabled.
   * Expected Outcome: Read should fail due to checksum mismatch caused by WASB write, verifying integrity enforcement.
   */
  @Test
  public void testScenario6() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    assumeBlobServiceType();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    abfs.create(path, true);
    try (FSDataOutputStream nativeFsStream = wasb.append(path)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 7: Create a file using WASB and then overwrite it using ABFS with overwrite=true.
   * Expected Outcome: ABFS should successfully overwrite the existing file created by WASB without error.
   */
  @Test
  public void testScenario7() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    abfs.create(path, true);
    FileStatus fileStatus = abfs.getFileStatus(path);
    Assertions.assertThat(fileStatus.getLen())
        .as("Expected file length to be 0 after overwrite")
        .isEqualTo(0L);

    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 8: Create a file using WASB and then attempt to create the same file using ABFS with overwrite=false.
   * Expected Outcome: ABFS should fail to create the file due to the file already existing.
   */
  @Test
  public void testScenario8() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs)).isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    try {
      abfs.create(path, false);
    } catch (IOException e) {
      AbfsRestOperationException restEx = (AbfsRestOperationException) e.getCause();
      if (restEx != null) {
        Assertions.assertThat(restEx.getStatusCode())
            .as("Expected HTTP status code 409 (Conflict) when file already exists")
            .isEqualTo(HTTP_CONFLICT);
      }
      Assertions.assertThat(e.getMessage())
          .as("Expected error message to contain 'AlreadyExists'")
          .contains("AlreadyExists");
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 9: Create a file using ABFS and then attempt to create the same file using WASB with overwrite=true.
   * Expected Outcome: WASB should successfully overwrite the existing file.
   */
  @Test
  public void testScenario9() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    try (FSDataOutputStream abfsOutputStream = abfs.create(path, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    wasb.create(path, true);
    FileStatus fileStatus = abfs.getFileStatus(path);
    Assertions.assertThat(fileStatus.getLen())
        .as("Expected file length to be 0 after overwrite")
        .isEqualTo(0L);

    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 10: Create a file using ABFS and then attempt to create the same file using WASB with overwrite=false.
   * Expected Outcome: WASB should fail to create the file as it already exists. The exception should indicate
   *  an "AlreadyExists" error with HTTP status code 409 (Conflict).
   */
  @Test
  public void testScenario10() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    try (FSDataOutputStream abfsOutputStream = abfs.create(path, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    try {
      wasb.create(path, false);
    } catch (IOException e) {
      AbfsRestOperationException restEx
          = (AbfsRestOperationException) e.getCause();
      if (restEx != null) {
        Assertions.assertThat(restEx.getStatusCode())
            .as("Expected HTTP status code 409 (Conflict) when file already exists")
            .isEqualTo(HTTP_CONFLICT);
      }
      Assertions.assertThat(e.getMessage())
          .as("Expected error message to contain 'exists'")
          .contains("exists");
    }
    // Remove file
    assertDeleted(abfs, path, true);
  }

  /**
   * Scenario 11: Create a file using ABFS, write data to it using WASB, and then delete the file using ABFS.
   * Expected Outcome: File should be created via ABFS and writable by WASB.
   * ABFS delete should succeed, and the file should no longer exist.
   */
  @Test
  public void testScenario11() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    abfs.create(path, true);
    try (FSDataOutputStream nativeFsStream = wasb.append(path)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    abfs.delete(path, true);
  }

  /**
   * Scenario 12: Create and write a file using ABFS, and then delete the same file using WASB.
   * Expected Outcome: File should be created and written successfully via ABFS.
   * WASB should be able to delete the file without errors.
   */
  @Test
  public void testScenario12() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream abfsOutputStream = abfs.create(path, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    wasb.delete(path, true);
  }

  /**
   * Scenario 13: Create a file using ABFS, write data to it using WASB, and then read the file using WASB.
   * Expected Outcome: The read operation via WASB should return the correct content written via WASB.
   */
  @Test
  public void testScenario13() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    abfs.create(path, true);
    try (FSDataOutputStream nativeFsStream = wasb.append(path)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(wasb.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + wasb);
    }
    abfs.delete(path, true);
  }

  /**
   * Scenario 14: Create a file using ABFS, write data to it using WASB, and delete the file using WASB.
   * Expected Outcome: Write via WASB should succeed and data should be persisted; delete via WASB should succeed without errors.
   */
  @Test
  public void testScenario14() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    abfs.create(path, true);
    try (FSDataOutputStream nativeFsStream = wasb.append(path)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(wasb.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + wasb);
    }
    wasb.delete(path, true);
  }

  /**
   * Scenario 15: Create and write a file using WASB, then delete the file using ABFS.
   * Expected Outcome: Write via WASB should succeed and data should be persisted; delete via ABFS should succeed without errors.
   */
  @Test
  public void testScenario15() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(wasb.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + wasb);
    }
    abfs.delete(path, true);
  }

  /**
   * Scenario 16: Create a file using WASB, write data to it using ABFS, and then delete the file using WASB.
   * Expected Outcome: Write via ABFS should succeed and persist data; delete via WASB should succeed without errors.
   */
  @Test
  public void testScenario16() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    wasb.create(path, true);
    try (FSDataOutputStream abfsOutputStream = abfs.append(path)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    wasb.delete(path, true);
  }

  /**
   * Scenario 17: Create a file using ABFS, set attribute (xAttr), and retrieve it using ABFS.
   * Expected Outcome: setXAttr and getXAttr operations via ABFS should succeed and return the correct value.
   */
  @Test
  public void testScenario17() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream abfsOutputStream = abfs.create(path, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = abfs.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    abfs.delete(path, true);
  }

  /**
   * Scenario 18: Create a file using WASB, set an attribute (xAttr), and retrieve it using WASB.
   * Expected Outcome: setXAttr and getXAttr operations via WASB should succeed and return the correct value.
   */
  @Test
  public void testScenario18() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs,  readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  /**
   * Scenario 19: Create a file using WASB, set an attribute using WASB, and retrieve it using ABFS.
   * Expected Outcome: Attribute set via WASB should be retrievable via ABFS and should match the original value.
   */
  @Test
  public void testScenario19() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  /**
   * Scenario 20: Create a file using WASB, set an attribute via WASB, retrieve the attribute via ABFS,
   * and then create the file again using ABFS with overwrite=true.
   * Expected Outcome: Attribute set via WASB should be retrievable via ABFS before overwrite.
   * After overwrite via ABFS, the attribute should no longer exist.
   */
  @Test
  public void testScenario20() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    abfs.create(path, true);
    FileStatus fileStatus = abfs.getFileStatus(path);
    Assertions.assertThat(fileStatus.getLen())
        .as("Expected file length to be 0 after overwrite")
        .isEqualTo(0L);
    wasb.delete(path, true);
  }

  /**
   * Scenario 21: Create a file using ABFS, set an attribute via ABFS, retrieve the attribute via WASB,
   * and then create the file again using WASB with overwrite=true.
   * Expected Outcome: Attribute set via ABFS should be retrievable via WASB before overwrite.
   * After overwrite via WASB, the attribute should no longer exist.
   */
  @Test
  public void testScenario21() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream abfsOutputStream = abfs.create(path, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.create(path, true);
    FileStatus fileStatus = abfs.getFileStatus(path);
    Assertions.assertThat(fileStatus.getLen())
        .as("Expected file length to be 0 after overwrite")
        .isEqualTo(0L);
    wasb.delete(path, true);
  }

  /**
   * Scenario 22: Create a file using WASB, set an attribute via ABFS,
   * retrieve the attribute via WASB, and then create the file again using WASB with overwrite=true.
   * Expected Outcome: Attribute set via ABFS should be retrievable via WASB before overwrite.
   * After overwrite via WASB, the attribute should be removed.
   */
  @Test
  public void testScenario22() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.create(path, true);
    FileStatus fileStatus = abfs.getFileStatus(path);
    Assertions.assertThat(fileStatus.getLen())
        .as("Expected file length to be 0 after overwrite")
        .isEqualTo(0L);
    wasb.delete(path, true);
  }

  /**
   * Scenario 23: Create a file using WASB, set an attribute via ABFS,
   * then set another attribute via WASB, and retrieve attributes via ABFS.
   * Expected Outcome: Both attributes should be retrievable via ABFS,
   * confirming that updates from both ABFS and WASB are visible.
   */
  @Test
  public void testScenario23() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2, CREATE_FLAG);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  /**
   * Scenario 24: Create a file using ABFS, then set an attribute via WASB,
   * and retrieve the attribute via ABFS.
   * Expected Outcome: Attribute set via WASB should be retrievable via ABFS,
   * verifying cross-compatibility of attribute operations.
   */
  @Test
  public void testScenario24() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream abfsOutputStream = abfs.create(path, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2, CREATE_FLAG);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  /**
   * Scenario 25: Create a file using WASB, then set and retrieve an attribute via ABFS,
   * and finally delete the file using WASB.
   * Expected Outcome: Attribute set via ABFS should be retrievable via ABFS,
   * and file deletion via WASB should succeed.
   */
  @Test
  public void testScenario25() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2, CREATE_FLAG);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  /**
   * Scenario 26: Create a file using ABFS, then set and retrieve an attribute via WASB,
   * and finally delete the file using WASB.
   * Expected Outcome: Attribute set via WASB should be retrievable via WASB,
   * and file deletion via WASB should succeed.
   */
  @Test
  public void testScenario26() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream abfsOutputStream = abfs.create(path, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = abfs.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2, CREATE_FLAG);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  /**
   * Scenario 27: Create and write a file using ABFS, then rename the file using WASB.
   * Expected Outcome: WASB should successfully rename the file created and written by ABFS.
   */
  @Test
  public void testScenario27() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream abfsOutputStream = abfs.create(testPath1, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME FILE ---
    boolean renamed = wasb.rename(testPath1, testPath2);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();

    // --- LIST FILES IN DIRECTORY ---
    Path parentDir = new Path(testFile + "/~12/!008");
    int noOfFiles = listAllFilesAndDirs(wasb, parentDir);
    Assertions.assertThat(noOfFiles)
        .as("Expected only 1 file or directory under path: %s", parentDir)
        .isEqualTo(1);
    wasb.delete(testPath2, true);
  }

  /**
   * Scenario 28: Create and write a file using WASB, rename the file using ABFS, and list files using ABFS.
   * Expected Outcome: ABFS should successfully rename the file created by WASB, and the renamed file should appear in listings.
   */
  @Test
  public void testScenario28() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME FILE ---
    boolean renamed = abfs.rename(testPath1, testPath2);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();

    // --- LIST FILES IN DIRECTORY ---
    Path parentDir = new Path(testFile + "/~12/!008");
    int noOfFiles = listAllFilesAndDirs(abfs, parentDir);
    Assertions.assertThat(noOfFiles)
        .as("Expected only 1 file or directory under path: %s", parentDir)
        .isEqualTo(1);
    wasb.delete(testPath2, true);
  }

  /**
   * Scenario 29: Create a file using WASB, write data to it via ABFS, rename the file using ABFS, and list files using ABFS.
   * Expected Outcome: ABFS should successfully rename the file and list the renamed file accurately.
   */
  @Test
  public void testScenario29() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    wasb.create(testPath1, true);
    try (FSDataOutputStream abfsOutputStream = abfs.append(testPath1)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME FILE ---
    boolean renamed = abfs.rename(testPath1, testPath2);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();

    // --- LIST FILES IN DIRECTORY ---
    Path parentDir = new Path(testFile + "/~12/!008");
    int noOfFiles = listAllFilesAndDirs(abfs, parentDir);
    Assertions.assertThat(noOfFiles)
        .as("Expected only 1 file or directory under path: %s", parentDir)
        .isEqualTo(1);
    wasb.delete(testPath2, true);
  }

  /**
   * Scenario 30: Create and write a file using WASB, rename it via WASB, rename again via ABFS, and list files using ABFS.
   * Expected Outcome: Both renames should succeed, and ABFS listing should reflect the latest filename.
   */
  @Test
  public void testScenario30() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath3 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME FILE ---
    boolean renamed = wasb.rename(testPath1, testPath2);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();

    // --- RENAME FILE ---
    boolean renamed1 = abfs.rename(testPath2, testPath3);
    Assertions.assertThat(renamed1)
        .as("Rename failed")
        .isTrue();

    // --- LIST FILES IN DIRECTORY ---
    Path parentDir = new Path(testFile + "/~12/!008");
    int noOfFiles = listAllFilesAndDirs(abfs, parentDir);
    Assertions.assertThat(noOfFiles)
        .as("Expected only 1 file or directory under path: %s", parentDir)
        .isEqualTo(1);
    wasb.delete(testPath3, true);
  }

  /**
   * Scenario 31: Create and write a file using WASB, delete it via WASB, then attempt to rename the deleted file via ABFS.
   * Expected Outcome: Rename should fail since the file was deleted, ensuring proper error handling.
   */
  @Test
  public void testScenario31() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    wasb.delete(testPath1, true);

    // --- RENAME FILE ---
    boolean renamed = abfs.rename(testPath1, testPath2);
    Assertions.assertThat(renamed)
        .as("Rename operation should have failed but returned true")
        .isFalse();
  }

  /**
   * Scenario 32: Create a directory and file using WASB, rename the directory using ABFS, and list files using ABFS.
   * Expected Outcome: ABFS should successfully rename the directory, and listing should reflect the updated directory name.
   */
  @Test
  public void testScenario32() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testFile1 = path("/testReadFile1");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath3 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    wasb.mkdirs(testFile);
    try (FSDataOutputStream nativeFsStream = wasb.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    wasb.create(testPath2, true);
    wasb.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME DIR ---
    boolean renamed = abfs.rename(testFile, testFile1);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();
    // --- LIST FILES IN DIRECTORY ---
    int listResult = listAllFilesAndDirs(abfs, testFile1);
    Assertions.assertThat(listResult)
        .as("Expected only 5 entries under path: %s", testFile1)
        .isEqualTo(5);
  }

  /**
   * Scenario 33: Create a directory and file using ABFS, rename the directory using WASB, and list files using WASB.
   * Expected Outcome: WASB should successfully rename the directory, and listing should reflect the updated directory name.
   */
  @Test
  public void testScenario33() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testFile1 = path("/testReadFile1");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath3 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    abfs.mkdirs(testFile);
    try (FSDataOutputStream abfsOutputStream = abfs.create(testPath1, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }
    abfs.create(testPath2, true);
    abfs.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME DIR ---
    boolean renamed = wasb.rename(testFile, testFile1);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();
    // --- LIST FILES IN DIRECTORY ---
    int listResult = listAllFilesAndDirs(wasb, testFile1);
    Assertions.assertThat(listResult)
        .as("Expected only 5 entries under path: %s", testFile1)
        .isEqualTo(5);
  }

  /**
   * Scenario 34: Create a directory via ABFS, rename a file inside the directory using WASB, and list files via ABFS.
   * Expected Outcome: WASB should successfully rename the file, and ABFS listing should reflect the updated filename.
   */
  @Test
  public void testScenario34() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath3 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    abfs.mkdirs(testFile);
    try (FSDataOutputStream abfsOutputStream = abfs.create(testPath1, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }
    abfs.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME DIR ---
    boolean renamed = wasb.rename(testPath1, testPath2);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();
    // --- LIST FILES IN DIRECTORY ---
    int listResult = listAllFilesAndDirs(abfs, testFile);
    Assertions.assertThat(listResult)
        .as("Expected only 4 entries under path: %s", testFile)
        .isEqualTo(4);
  }

  /**
   * Scenario 35: Create a directory via WASB, rename a file inside the directory using ABFS, and list files via WASB.
   * Expected Outcome: ABFS should successfully rename the file, and WASB listing should reflect the updated filename.
   */
  @Test
  public void testScenario35() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath3 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    wasb.mkdirs(testFile);
    try (FSDataOutputStream nativeFsStream = wasb.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    wasb.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME DIR ---
    boolean renamed = abfs.rename(testPath1, testPath2);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();
    // --- LIST FILES IN DIRECTORY ---
    int listResult = listAllFilesAndDirs(wasb, testFile);
    Assertions.assertThat(listResult)
        .as("Expected only 4 entries under path: %s", testFile)
        .isEqualTo(4);
  }

  /**
   * Scenario 36: Create a file via WASB, attempt to rename it to an existing filename using ABFS, and list files via WASB.
   * Expected Outcome: Rename should fail due to existing target name, and WASB listing should remain unchanged.
   */

  @Test
  public void testScenario36() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath3 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    wasb.mkdirs(testFile);
    try (FSDataOutputStream nativeFsStream = wasb.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    wasb.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME DIR ---
    boolean renamed = abfs.rename(testFile, testFile);
    Assertions.assertThat(renamed)
        .as("Rename operation should have failed but returned true")
        .isFalse();
  }

  /**
   * Scenario 37: Attempt to rename a non-existent file using WASB.
   * Expected Outcome: Rename operation should fail with an appropriate error indicating the file does not exist.
   */
  @Test
  public void testScenario37() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath3 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    abfs.mkdirs(testFile);
    try (FSDataOutputStream abfsOutputStream = abfs.create(testPath1, true)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }
    abfs.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME NON EXISTENT FILE ---
    boolean renamed = wasb.rename(testPath2, testPath3);
    Assertions.assertThat(renamed)
        .as("Rename operation should have failed but returned true")
        .isFalse();
  }

  /**
   * Scenario 38: Create a file using WASB, set and get an attribute via WASB, then create the file again with overwrite=true using WASB.
   * Expected Outcome: Attribute operations should succeed before overwrite, and after overwrite, the file should be replaced with no prior attributes.
   */
  @Test
  public void testScenario38() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    try (FSDataOutputStream nativeFsStream = wasb.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = wasb.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    ITestAzureBlobFileSystemAttributes.assertAttributeEqual(abfs, readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.create(path, true);
    FileStatus fileStatus = abfs.getFileStatus(path);
    Assertions.assertThat(fileStatus.getLen())
        .as("Expected file length to be 0 after overwrite")
        .isEqualTo(0L);
    wasb.delete(path, true);
  }

  /**
   * Scenario 39: Create and write a file using WASB, rename the file using WASB, and list files using WASB.
   * Expected Outcome: WASB should successfully rename the file, and the renamed file should appear in the listing.
   */
  @Test
  public void testScenario39() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    assumeThat(getIsNamespaceEnabled(abfs))
        .as("Namespace enabled account does not support this test")
        .isFalse();
    NativeAzureFileSystem wasb = getWasbFileSystem();

    String testRunId = UUID.randomUUID().toString();
    Path baseDir = path("/testScenario39_" + testRunId);
    Path testFile = new Path(baseDir, "testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath2 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());
    Path testPath3 = new Path(testFile + "/~12/!008/testfile_" + UUID.randomUUID());

    // Write
    wasb.mkdirs(testFile);
    try (FSDataOutputStream nativeFsStream = wasb.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    wasb.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals(TEST_CONTEXT, line, "Wrong text from " + abfs);
    }
    // --- RENAME DIR ---
    boolean renamed = wasb.rename(testPath1, testPath2);
    Assertions.assertThat(renamed)
        .as("Rename failed")
        .isTrue();
    // --- LIST FILES IN DIRECTORY ---
    int listResult = listAllFilesAndDirs(wasb, testFile);
    Assertions.assertThat(listResult)
        .as("Expected only 4 entries under path: %s", testFile)
        .isEqualTo(4);
  }

  /**
   * Recursively counts all files and directories under the given path.
   *
   * @param fs   The file system to use.
   * @param path The starting path.
   * @return Total number of files and directories.
   * @throws IOException If an error occurs while accessing the file system.
   */
  public static int listAllFilesAndDirs(FileSystem fs, Path path) throws IOException {
    int count = 0;
    RemoteIterator<FileStatus> iter = fs.listStatusIterator(path);

    while (iter.hasNext()) {
      FileStatus status = iter.next();
      count++; // Count this file or directory

      if (status.isDirectory()) {
        count += listAllFilesAndDirs(fs, status.getPath()); // Recurse into directory
      }
    }

    return count;
  }

  /**
   * Checks that the given path is a regular file (not a directory or symlink).
   *
   * @param path   The file path.
   * @param status The file status.
   * @throws AssertionError If the path is a directory or a symlink.
   */
  private static void assertIsFile(Path path, FileStatus status) {
    Assertions.assertThat(status.isDirectory())
        .as("Expected a regular file, but was a directory: %s %s", path, status)
        .isFalse();

    Assertions.assertThat(status.isSymlink())
        .as("Expected a regular file, but was a symlink: %s %s", path, status)
        .isFalse();
  }
}
