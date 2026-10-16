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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.tosfs.RawFileSystem;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

public class TestChecksum extends AbstractFSContractTestBase {
  @BeforeAll
  public static void before() {
    Assumptions.assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new TosContract(conf);
  }

  private Path testCreateNewFile(String fileName, byte[] data, boolean useBuilder)
      throws IOException {
    describe("Foundational 'create a file' test, using builder API=" + useBuilder);
    Path path = path(fileName, useBuilder);

    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, false, useBuilder);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);

    return path;
  }

  private Path path(String filepath, boolean useBuilder) throws IOException {
    return super.path(filepath + (useBuilder ? "" : "-builder"));
  }

  @Test
  public void testCheckSumWithSimplePut() throws IOException {
    byte[] data = dataset(256, 'a', 'z');
    Path path1 = testCreateNewFile("file1", data, true);
    Path path2 = testCreateNewFile("file2", data, true);
    Path path3 = testCreateNewFile("file3", dataset(512, 'a', 'z'), true);

    FileChecksum expected = getFileSystem().getFileChecksum(path1);
    assertEquals(expected, getFileSystem().getFileChecksum(path2),
        "Checksum value should be same among objects with same content");
    assertEquals(expected, getFileSystem().getFileChecksum(path1),
        "Checksum value should be same among multiple call for same object");
    assertNotEquals(expected, getFileSystem().getFileChecksum(path3),
        "Checksum value should be different for different objects with different content");

    Path renamed = path("renamed");
    getFileSystem().rename(path1, renamed);
    assertEquals(expected, getFileSystem().getFileChecksum(renamed),
        "Checksum value should not change after rename");
  }

  @Test
  public void testCheckSumShouldSameViaPutAndMPU() throws IOException {
    byte[] data = TestUtility.rand(11 << 20);

    // simple put
    Path singleFile = path("singleFile");
    RawFileSystem fs = (RawFileSystem) getFileSystem();
    fs.storage().put(ObjectUtils.pathToKey(singleFile), data);

    // MPU upload data, the default threshold is 10MB
    Path mpuFile = testCreateNewFile("mpuFile", data, true);

    assertEquals(fs.getFileChecksum(singleFile), fs.getFileChecksum(mpuFile));
  }

  @Test
  public void testDisableCheckSum() throws IOException {
    Path path1 = testCreateNewFile("file1", dataset(256, 'a', 'z'), true);
    Path path2 = testCreateNewFile("file2", dataset(512, 'a', 'z'), true);
    assertNotEquals(getFileSystem().getFileChecksum(path1), getFileSystem().getFileChecksum(path2));

    // disable checksum
    Configuration newConf = new Configuration(getFileSystem().getConf());
    newConf.setBoolean(ConfKeys.FS_CHECKSUM_ENABLED.key("tos"), false);
    FileSystem newFS = FileSystem.get(newConf);

    assertEquals(newFS.getFileChecksum(path1), newFS.getFileChecksum(path2));
  }

  @Test
  public void testGetDirChecksum() throws IOException {
    FileSystem fs = getFileSystem();

    Path dir1 = path("dir1", true);
    Path dir2 = path("dir2", true);
    assertPathDoesNotExist("directory already exists", dir1);
    assertPathDoesNotExist("directory already exists", dir2);
    fs.mkdirs(dir1);

    assertThrows(FileNotFoundException.class, () -> getFileSystem().getFileChecksum(dir1),
        "Path is not a file");
    assertThrows(FileNotFoundException.class, () -> getFileSystem().getFileChecksum(dir2),
        "No such file or directory");

    assertDeleted(dir1, false);
  }
}
