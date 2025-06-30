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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
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
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INFINITE_LEASE_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDeleted;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.file;

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
    Assume.assumeFalse("Emulator is not supported", isIPAddress());
  }

  @Test
  public void testListFileStatus() throws Exception {
    // crate file using abfs
    AzureBlobFileSystem fs = getFileSystem();
    // test only valid for non-namespace enabled account
    Assume.assumeFalse("Namespace enabled account does not support this test,",
        getIsNamespaceEnabled(fs));
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());

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
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());

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
        assertEquals("Wrong text from " + readFs,
            TEST_CONTEXT, line);
      }

      // Remove file
      assertDeleted(readFs, path, true);
    }
  }

  @Test
  public void testwriteFile() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 1);
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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    try (FSDataOutputStream nativeFsStream = abfs.append(path)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
  }

  @Test
  public void testwriteFile1() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 2);
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
  }

  @Test
  public void testazcopywasbcompatibility() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 2);
    createAzCopyFile(path);

    try (FSDataOutputStream nativeFsStream = abfs.append(path)) {
      nativeFsStream.write(TEST_CONTEXT1.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
  }


  @Test
  public void testDir() throws Exception {
    boolean[] createDirWithAbfs = new boolean[]{false, true, false, true};
    boolean[] readDirWithAbfs = new boolean[]{false, true, true, false};

    AzureBlobFileSystem abfs = getFileSystem();
    // test only valid for non-namespace enabled account
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));

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
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));

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

  //Scenario 1: - Create and write via WASB, read via ABFS
  @Test
  public void testScenario1() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 1);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  //Scenario 2: - Create and write via WASB, read via ABFS and then write the same file via ABFS
  @Test
  public void testScenario2() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 2);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
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

  //Scenario 3: - Create and write via ABFS and the read via WASB
  @Test
  public void testScenario3() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 3);

    // Write
    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(wasb.open(path)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + wasb,
          TEST_CONTEXT, line);
    }
    // Remove file
    assertDeleted(abfs, path, true);
  }

  //Scenario 4:- Create via WASB, write via ABFS and then write via WASB
  @Test
  public void testScenario4() throws Exception {
    AzureBlobFileSystem abfs = getFileSystem();
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 4);

    // Write
    wasb.create(path, true);
    try (FSDataOutputStream abfsOutputStream = abfs.append(path)) {
      abfsOutputStream.write(TEST_CONTEXT.getBytes());
      abfsOutputStream.flush();
      abfsOutputStream.hsync();
    }

    try (FSDataOutputStream nativeFsStream = abfs.append(path)) {
      nativeFsStream.write(TEST_CONTEXT1.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);
    // Remove file
    assertDeleted(abfs, path, true);
  }

  //Scenario 5:- Create via ABFS, write via WASB, read via ABFS (Checksum validation disabled)
  @Test
  public void testScenario5() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, false);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 5);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  //Scenario 6: - Create via ABFS, write via WASB, read via ABFS (Checksum validation enabled)
  @Test
  public void testScenario6() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 6);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  // Scenario 7 :- Create via WASB and then create overwrite true using ABFS
  @Test
  public void testScenario7() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 7);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    abfs.create(path, true);

    // Remove file
    assertDeleted(abfs, path, true);
  }

  // Scenario 8 :- Create via WASB and then create overwrite false using ABFS
  @Test
  public void testScenario8() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 8);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    try {
      abfs.create(path, false);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("AlreadyExists"));
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  // Scenario 9 :- Create via ABFS and then create overwrite true using WASB
  @Test
  public void testScenario9() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 9);

    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    wasb.create(path, true);

    // Remove file
    assertDeleted(abfs, path, true);
  }

  // Scenario 10 :- Create via ABFS and then create overwrite false using WASB
  @Test
  public void testScenario10() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 10);

    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    try {
      wasb.create(path, false);
    } catch (Exception e) {
      assertTrue(e.getMessage().toLowerCase().contains("exists"));
    }

    // Remove file
    assertDeleted(abfs, path, true);
  }

  // Scenario 11 :- Create via ABFS and then write via WASB and delete via ABFS
  @Test
  public void testScenario11() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 11);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    abfs.delete(path, true);
  }

  // Scenario 12 :- Create and write via ABFS and delete via WASB
  @Test
  public void testScenario12() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 12);

    // Write
    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    wasb.delete(path, true);
  }

  // Scenario 13:- Create via ABFS, write via WASB, and read via wasb
  @Test
  public void testScenario13() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 13);

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
      assertEquals("Wrong text from " + wasb,
          TEST_CONTEXT, line);
    }
    abfs.delete(path, true);
  }

  // Scenario 14:- Create via ABFS, write via WASB, and delete via wasb
  @Test
  public void testScenario14() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 14);

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
      assertEquals("Wrong text from " + wasb,
          TEST_CONTEXT, line);
    }
    wasb.delete(path, true);
  }

  // Scenario 15 :- Create and write via WASB and delete via ABFS
  @Test
  public void testScenario15() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 15);

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
      assertEquals("Wrong text from " + wasb,
          TEST_CONTEXT, line);
    }
    abfs.delete(path, true);
  }

  // Scenario 16: Create via WASB, write via ABFS, and delete via WASB
  @Test
  public void testScenario16() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 16);

    // Write
    wasb.create(path, true);
    try (FSDataOutputStream nativeFsStream = abfs.append(path)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, path);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(path)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    wasb.delete(path, true);
  }

  // Scenario 17: Create, setXAttr and getXAttr via ABFS
  @Test
  public void testScenario17() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 17);
    // Write
    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = abfs.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    abfs.delete(path, true);
  }

  // Scenario 17: Create, setXAttr and getXAttr via WASB
  @Test
  public void testScenario18() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 18);
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  // Scenario 19: Create, setXAttr via wasb and getXAttr via ABFS
  @Test
  public void testScenario19() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 19);
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  // Scenario 20: Create, setXAttr via wasb and getXAttr via ABFS and create overwrite via ABFS
  @Test
  public void testScenario20() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 20);
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    abfs.create(path, true);
    wasb.delete(path, true);
  }

  // Scenario 21: Create, setXAttr ABFS, getXAttr WASB and create overwrite via WASB
  @Test
  public void testScenario21() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 21);
    // Write
    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.create(path, true);
    wasb.delete(path, true);
  }

  // Scenario 22: Create via WASB, setXAttr ABFS, getXAttr wasb and create overwrite via WASB
  @Test
  public void testScenario22() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 22);
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.create(path, true);
    wasb.delete(path, true);
  }

  // Scenario 23: Create via WASB, setXAttr ABFS, then setXAttr via WASB and getXAttr via ABFS
  @Test
  public void testScenario23() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 23);
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2, CREATE_FLAG);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  // Scenario 24: Create via ABFS, then setXAttr via WASB and getXAttr via ABFS
  @Test
  public void testScenario24() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 24);
    // Write
    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2, CREATE_FLAG);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  // Scenario 24: Create via WASB, then setXAttr getXAttr via ABFS and delete via WASB
  @Test
  public void testScenario25() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 25);
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    abfs.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2, CREATE_FLAG);
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = abfs.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  // Scenario 26: Create via ABFS, then setXAttr getXAttr via WASB and delete via WASB
  @Test
  public void testScenario26() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 26);
    // Write
    try (FSDataOutputStream nativeFsStream = abfs.create(path, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // --- VALIDATE FILE ---
    FileStatus status = abfs.getFileStatus(path);
    assertIsFile(path, status);

    // --- SET XATTR #1 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_1, ATTRIBUTE_VALUE_1);
    byte[] readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2, CREATE_FLAG);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.delete(path, true);
  }

  // Scenario 27: Create and write via ABFS, rename via wasb
  @Test
  public void testScenario27() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 27);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 28);

    // Write
    try (FSDataOutputStream nativeFsStream = abfs.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME FILE ---
    boolean renamed = wasb.rename(testPath1, testPath2);
    System.out.println("Rename successful: " + renamed);

    // --- LIST FILES IN DIRECTORY ---
    Path parentDir = new Path(testFile + "/~12/!008");
    FileStatus[] fileStatuses = wasb.listStatus(parentDir);

    for (FileStatus status : fileStatuses) {
      System.out.println("File: " + status.getPath());
    }
    wasb.delete(testPath2, true);
  }

  // Scenario 28: Create and write via WASB, rename via ABFS, list via ABFS
  @Test
  public void testScenario28() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 29);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 30);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME FILE ---
    boolean renamed = abfs.rename(testPath1, testPath2);
    System.out.println("Rename successful: " + renamed);

    // --- LIST FILES IN DIRECTORY ---
    Path parentDir = new Path(testFile + "/~12/!008");
    FileStatus[] fileStatuses = abfs.listStatus(parentDir);

    for (FileStatus status : fileStatuses) {
      System.out.println("File: " + status.getPath());
    }
    wasb.delete(testPath2, true);
  }

  // Scenario 29: Create via WASB and write via ABFS, rename via ABFS, list via ABFS
  @Test
  public void testScenario29() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 29);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 30);

    // Write
    wasb.create(testPath1, true);
    try (FSDataOutputStream nativeFsStream = abfs.append(testPath1)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME FILE ---
    boolean renamed = abfs.rename(testPath1, testPath2);
    System.out.println("Rename successful: " + renamed);

    // --- LIST FILES IN DIRECTORY ---
    Path parentDir = new Path(testFile + "/~12/!008");
    FileStatus[] fileStatuses = abfs.listStatus(parentDir);

    for (FileStatus status : fileStatuses) {
      System.out.println("File: " + status.getPath());
    }
    wasb.delete(testPath2, true);
  }

  //Scenario 30: Create and write via WASB, rename via WASB, rename via ABFS, list via ABFS
  @Test
  public void testScenario30() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 31);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 32);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 33);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME FILE ---
    boolean renamed = wasb.rename(testPath1, testPath2);
    System.out.println("Rename successful: " + renamed);
    // --- RENAME FILE ---
    boolean renamed1 = abfs.rename(testPath2, testPath3);
    System.out.println("Rename successful: " + renamed1);

    // --- LIST FILES IN DIRECTORY ---
    Path parentDir = new Path(testFile + "/~12/!008");
    FileStatus[] fileStatuses = abfs.listStatus(parentDir);

    for (FileStatus status : fileStatuses) {
      System.out.println("File: " + status.getPath());
    }
    wasb.delete(testPath3, true);
  }

  //Scenario 31: Create and write via WASB, delete via WASB, rename via ABFS -> should fail
  @Test
  public void testScenario31() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 31);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 32);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 33);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    wasb.delete(testPath1, true);
    // --- RENAME FILE ---
    boolean renamed = abfs.rename(testPath1, testPath2);
    System.out.println("Rename successful: " + renamed);
    Assertions.assertThat(renamed)
        .as("Rename operation should have failed but returned true")
        .isFalse();
  }

  //Scenario 32 :Create Dir & File via WASB → Rename Dir via ABFS → List Files via ABFS
  @Test
  public void testScenario32() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testFile1 = path("/testReadFile1");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 50);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 51);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 52);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME DIR ---
    boolean renamed = abfs.rename(testFile, testFile1);
    System.out.println("Rename successful: " + renamed);
    // --- LIST FILES IN DIRECTORY ---
   listAllFilesAndDirs(abfs, testFile1);
  }

  //Scenario 33 :Create Dir & File via ABFS → Rename Dir via WASB → List Files via WASB
  @Test
  public void testScenario33() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testFile1 = path("/testReadFile1");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 55);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 56);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 57);

    // Write
    abfs.mkdirs(testFile);
    try (FSDataOutputStream nativeFsStream = abfs.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    abfs.create(testPath2, true);
    abfs.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME DIR ---
    boolean renamed = wasb.rename(testFile, testFile1);
    System.out.println("Rename successful: " + renamed);
    // --- LIST FILES IN DIRECTORY ---
    listAllFilesAndDirs(wasb, testFile1);
  }

  //Scenario 34: Create Dir via ABFS → Rename File inside Dir via WASB → List via ABFS
  @Test
  public void testScenario34() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testFile1 = path("/testReadFile1");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 65);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 66);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 67);

    // Write
    abfs.mkdirs(testFile);
    try (FSDataOutputStream nativeFsStream = abfs.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    abfs.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME DIR ---
    boolean renamed = wasb.rename(testPath1, testPath2);
    System.out.println("Rename successful: " + renamed);
    // --- LIST FILES IN DIRECTORY ---
    listAllFilesAndDirs(abfs, testFile);
  }

  //Scenario 35: Create Dir via WASB → Rename File inside Dir via ABFS → List via WASB
  @Test
  public void testScenario35() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testFile1 = path("/testReadFile1");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 75);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 76);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 77);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME DIR ---
    boolean renamed = abfs.rename(testPath1, testPath2);
    System.out.println("Rename successful: " + renamed);
    // --- LIST FILES IN DIRECTORY ---
    listAllFilesAndDirs(wasb, testFile);
  }

  //Scenario 36: Create via WASB → Rename to existing name via ABFS → List via WASB
  @Test
  public void testScenario36() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testFile1 = path("/testReadFile1");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 75);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 76);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 77);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME DIR ---
    boolean renamed = abfs.rename(testFile, testFile);
    System.out.println("Rename successful: " + renamed);
    Assertions.assertThat(renamed)
        .as("Rename operation should have failed but returned true")
        .isFalse();
  }

  //Scenario 37: Rename a non-existent file via WASB
  @Test
  public void testScenario37() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path testFile1 = path("/testReadFile1");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 75);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 76);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 77);

    // Write
    abfs.mkdirs(testFile);
    try (FSDataOutputStream nativeFsStream = abfs.create(testPath1, true)) {
      nativeFsStream.write(TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    abfs.create(testPath3, true);

    // Check file status
    ContractTestUtils.assertIsFile(abfs, testPath1);

    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(abfs.open(testPath1)))) {
      String line = br.readLine();
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME NON EXISTENT FILE ---
    boolean renamed = wasb.rename(testPath2, testPath3);
    System.out.println("Rename successful: " + renamed);
    Assertions.assertThat(renamed)
        .as("Rename operation should have failed but returned true")
        .isFalse();
  }

  // Scenario 38: Create via WASB, setXAttr and getXAttr WASB and create overwrite via WASB
  @Test
  public void testScenario38() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path testFile = path("/testReadFile");
    Path path = new Path(testFile + "/~12/!008/testfile" + 38);
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
    System.out.println("XAttr raw bytes: " + Arrays.toString(readValue));
    System.out.println("XAttr as string: " + new String(readValue, StandardCharsets.UTF_8));
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    // --- SET XATTR #2 ---
    wasb.setXAttr(path, ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2);
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_2);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_2, "two");

    // --- VERIFY XATTR #1 AGAIN ---
    readValue = wasb.getXAttr(path, ATTRIBUTE_NAME_1);
    assertAttributeEqual(readValue, ATTRIBUTE_VALUE_1, "one");

    wasb.create(path, true);
    wasb.delete(path, true);
  }

  // Scenario 39: Create and write via WASB, rename via wasb and list via WASB
  @Test
  public void testScenario39() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
    Assume.assumeFalse("Namespace enabled account does not support this test",
        getIsNamespaceEnabled(abfs));
    NativeAzureFileSystem wasb = getWasbFileSystem();

    String testRunId = UUID.randomUUID().toString();
    Path baseDir = path("/testScenario39_" + testRunId);
    Path testFile = new Path(baseDir, "testReadFile");
    Path testPath1 = new Path(testFile + "/~12/!008/testfile" + 1);
    Path testPath2 = new Path(testFile + "/~12/!008/testfile" + 2);
    Path testPath3 = new Path(testFile + "/~12/!008/testfile" + 3);

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
      assertEquals("Wrong text from " + abfs,
          TEST_CONTEXT, line);
    }
    // --- RENAME DIR ---
    boolean renamed = wasb.rename(testPath1, testPath2);
    System.out.println("Rename successful: " + renamed);
    // --- LIST FILES IN DIRECTORY ---
    listAllFilesAndDirs(wasb, testFile);
  }

  public static void listAllFilesAndDirs(FileSystem fs, Path path) throws
      IOException {
    RemoteIterator<FileStatus> iter = fs.listStatusIterator(path);

    while (iter.hasNext()) {
      FileStatus status = iter.next();

      if (status.isDirectory()) {
        System.out.println("Directory: " + status.getPath());
        // Recursive call
        listAllFilesAndDirs(fs, status.getPath());
      } else {
        System.out.println("File: " + status.getPath());
      }
    }
  }

  private static void assertIsFile(Path path, FileStatus status) {
    if (status.isDirectory()) {
      throw new AssertionError("File claims to be a directory: " + path + " " + status);
    }
    if (status.isSymlink()) {
      throw new AssertionError("File claims to be a symlink: " + path + " " + status);
    }
  }

  private static void assertAttributeEqual(byte[] actual, byte[] expected, String expectedDecoded) {
    if (!Arrays.equals(actual, expected)) {
      throw new AssertionError("Encoded attribute does not match expected bytes");
    }
    String decoded = new String(actual, StandardCharsets.UTF_8);
    if (!decoded.equals(expectedDecoded)) {
      throw new AssertionError("Decoded attribute does not match. Got: " + decoded + ", Expected: " + expectedDecoded);
    }
  }
}
