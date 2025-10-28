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
package org.apache.hadoop.fs.tosfs.contract;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

public class TestRename extends AbstractContractRenameTest {

  @BeforeAll
  public static void before() {
    Assumptions.assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    // Add follow two keys into hadoop configuration.
    String defaultScheme = FileSystem.getDefaultUri(conf).getScheme();
    Configuration newConf = new Configuration(conf);
    newConf.setLong(ConfKeys.FS_MULTIPART_SIZE.key(defaultScheme),
        ConfKeys.FS_MULTIPART_SIZE_DEFAULT);
    newConf.setLong(ConfKeys.FS_MULTIPART_THRESHOLD.key(defaultScheme),
        ConfKeys.FS_MULTIPART_THRESHOLD_DEFAULT);

    return new TosContract(newConf);
  }

  @Test
  public void testSucceedRenameFile() throws IOException {
    describe("check if source file and dest file exists when succeed to rename");
    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");
    FileSystem fs = getFileSystem();
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(fs, renameSrc, data, data.length, 1024 * 1024, true);
    boolean renamed = rename(renameSrc, renameDest);
    assertTrue(renamed);
    assertPathExists("dest file should exist when succeed to rename", renameDest);
    assertPathDoesNotExist("source file should not exist when succeed to rename", renameSrc);
  }

  @Test
  public void testSucceedRenameDir() throws IOException {
    describe("check if source dir and dest dir exists when succeed to rename");
    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");
    int fileNums = 10;
    int byteSize = 10 << 20; // trigger multipart upload
    FileSystem fs = getFileSystem();
    for (int i = 0; i < fileNums; i++) {
      byte[] data = dataset(byteSize >> i, 'a', 'z');
      writeDataset(fs, new Path(renameSrc, String.format("src%02d", i)), data, data.length,
          1024 * 1024, true);
    }
    boolean renamed = rename(renameSrc, renameDest);
    assertTrue(renamed);
    for (int i = 0; i < fileNums; i++) {
      Path srcFilePath = new Path(renameSrc, String.format("src%02d", i));
      Path dstFilePath = new Path(renameDest, String.format("src%02d", i));
      byte[] data = dataset(byteSize >> i, 'a', 'z');
      assertPathExists("dest file should exist when succeed to rename", dstFilePath);
      assertPathDoesNotExist("source file should not exist when succeed to rename", srcFilePath);
      try (InputStream is = fs.open(dstFilePath)) {
        assertArrayEquals(data, IOUtils.toByteArray(is));
      }
    }
  }

  @Test
  public void testFailedRename() throws IOException {
    describe("check if source file and dest file exists when failed to rename");
    Path renameSrc = path("src/renameSrc");
    Path renameDest = path("src/renameSrc/renameDst");
    FileSystem fs = getFileSystem();
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(fs, renameSrc, data, data.length, 1024 * 1024, true);
    boolean renamed;
    try {
      renamed = rename(renameSrc, renameDest);
    } catch (IOException e) {
      renamed = false;
    }
    assertFalse(renamed);
    assertPathExists("source file should exist when failed to rename", renameSrc);
    assertPathDoesNotExist("dest file should not exist when failed to rename", renameDest);
  }

  @Test
  public void testRenameSmallFile() throws IOException {
    testRenameFileByPut(1 << 20);
    testRenameFileByPut(3 << 20);
  }

  @Test
  public void testRenameLargeFile() throws IOException {
    testRenameFileByUploadParts(16 << 20);
    testRenameFileByUploadParts(10 << 20);
  }

  @Test
  public void testRenameDirWithSubFileAndSubDir() throws IOException {
    FileSystem fs = getFileSystem();

    Path renameSrc = path("dir/renameSrc");
    Path renameDest = path("dir/renameDst");
    int size = 1024;
    byte[] data = dataset(size, 'a', 'z');
    String fileName = "file.txt";
    writeDataset(fs, new Path(renameSrc, fileName), data, data.length, 1024, true);

    String dirName = "dir";
    Path dirPath = new Path(renameSrc, dirName);
    mkdirs(dirPath);
    assertPathExists("source dir should exist", dirPath);

    boolean renamed = fs.rename(renameSrc, renameDest);

    assertTrue(renamed);
    Path srcFilePath = new Path(renameSrc, fileName);
    Path dstFilePath = new Path(renameDest, fileName);
    assertPathExists("dest file should exist when succeed to rename", dstFilePath);
    assertPathDoesNotExist("source file should not exist when succeed to rename", srcFilePath);

    assertPathExists("dest dir should exist when succeed to rename", new Path(renameDest, dirName));
    assertPathDoesNotExist("source dir should not exist when succeed to rename",
        new Path(renameSrc, dirName));

    ContractTestUtils.cleanup("TEARDOWN", fs, getContract().getTestPath());
  }

  public void testRenameFileByPut(int size) throws IOException {
    describe("check if use put method when rename file");
    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");
    FileSystem fs = getFileSystem();

    byte[] data = dataset(size, 'a', 'z');
    String fileName = String.format("%sMB.txt", size >> 20);
    writeDataset(fs, new Path(renameSrc, fileName), data, data.length, 1024 * 1024, true);
    boolean renamed = fs.rename(renameSrc, renameDest);

    assertTrue(renamed);
    Path srcFilePath = new Path(renameSrc, fileName);
    Path dstFilePath = new Path(renameDest, fileName);
    assertPathExists("dest file should exist when succeed to rename", dstFilePath);
    assertPathDoesNotExist("source file should not exist when succeed to rename", srcFilePath);

    assertPathExists("dest src should exist when succeed to rename", renameDest);
    assertPathDoesNotExist("source src should not exist when succeed to rename", renameSrc);

    try (InputStream is = fs.open(dstFilePath)) {
      assertArrayEquals(data, IOUtils.toByteArray(is));
    }
    ContractTestUtils.cleanup("TEARDOWN", fs, getContract().getTestPath());
  }

  @Test
  public void testCreateParentDirAfterRenameSubFile() throws IOException {
    FileSystem fs = getFileSystem();

    Path srcDir = path("srcDir");
    Path destDir = path("destDir");

    assertPathDoesNotExist("Src dir should not exist", srcDir);
    assertPathDoesNotExist("Dest dir should not exist", destDir);
    int size = 1 << 20;
    byte[] data = dataset(size, 'a', 'z');
    String fileName = String.format("%sMB.txt", size >> 20);
    Path srcFile = new Path(srcDir, fileName);
    Path destFile = new Path(destDir, fileName);
    writeDataset(fs, srcFile, data, data.length, 1024 * 1024, true);

    assertPathExists("Src file should exist", srcFile);
    assertPathExists("Src dir should exist", srcDir);

    mkdirs(destDir);
    assertPathExists("Dest dir should exist", destDir);

    boolean renamed = fs.rename(srcFile, destFile);
    assertTrue(renamed);

    assertPathExists("Dest file should exist", destFile);
    assertPathExists("Dest dir should exist", destDir);
    assertPathDoesNotExist("Src file should not exist", srcFile);
    assertPathExists("Src dir should exist", srcDir);
  }

  @Test
  public void testCreateParentDirAfterRenameSubDir() throws IOException {
    FileSystem fs = getFileSystem();

    Path srcDir = path("srcDir");
    Path destDir = path("destDir");

    assertPathDoesNotExist("Src dir should not exist", srcDir);
    assertPathDoesNotExist("Dest dir should not exist", destDir);

    String subDirName = String.format("subDir");
    Path srcSubDir = new Path(srcDir, subDirName);
    Path destDestDir = new Path(destDir, subDirName);
    mkdirs(srcSubDir);

    assertPathExists("Src sub dir should exist", srcSubDir);
    assertPathExists("Src dir should exist", srcDir);

    mkdirs(destDir);
    assertPathExists("Dest dir should exist", destDir);

    boolean renamed = fs.rename(srcSubDir, destDestDir);
    assertTrue(renamed);

    assertPathExists("Dest sub dir should exist", destDestDir);
    assertPathExists("Dest dir should exist", destDir);
    assertPathDoesNotExist("Src sub dir should not exist", srcSubDir);
    assertPathExists("Src sub dir should exist", srcDir);
  }

  public void testRenameFileByUploadParts(int size) throws IOException {
    describe("check if use upload parts method when rename file");
    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");
    FileSystem fs = getFileSystem();

    byte[] data = dataset(size, 'a', 'z');
    String fileName = String.format("%sMB.txt", size >> 20);
    writeDataset(fs, new Path(renameSrc, fileName), data, data.length, 1024 * 1024, true);
    boolean renamed = fs.rename(renameSrc, renameDest);

    assertTrue(renamed);
    Path srcFilePath = new Path(renameSrc, fileName);
    Path dstFilePath = new Path(renameDest, fileName);
    assertPathExists("dest file should exist when succeed to rename", dstFilePath);
    assertPathDoesNotExist("source file should not exist when succeed to rename", srcFilePath);

    try (InputStream is = fs.open(dstFilePath)) {
      assertArrayEquals(data, IOUtils.toByteArray(is));
    }
    ContractTestUtils.cleanup("TEARDOWN", fs, getContract().getTestPath());
  }

  @Disabled
  @Test
  public void testRenameFileUnderFileSubdir() {
  }

  @Disabled
  @Test
  public void testRenameFileUnderFile() {
  }
}
