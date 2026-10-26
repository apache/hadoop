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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

public abstract class TestGetFileStatus extends AbstractContractGetFileStatusTest {

  private final boolean getFileStatusEnabled;

  public TestGetFileStatus(boolean getFileStatusEnabled) {
    this.getFileStatusEnabled = getFileStatusEnabled;
  }

  @BeforeAll
  public static void before() {
    Assumptions.assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    conf.setBoolean(TosKeys.FS_TOS_GET_FILE_STATUS_ENABLED, getFileStatusEnabled);
    conf.setBoolean(ConfKeys.FS_ASYNC_CREATE_MISSED_PARENT.key("tos"), false);
    return new TosContract(conf);
  }

  @Test
  public void testDirModificationTimeShouldNotBeZero() throws IOException {
    FileSystem fs = getFileSystem();
    Path path = getContract().getTestPath();
    fs.delete(path, true);

    Path subfolder = path.suffix('/' + this.methodName.getMethodName() + "-" + UUIDUtils.random());
    mkdirs(subfolder);

    FileStatus fileStatus = fs.getFileStatus(path);
    assertTrue(fileStatus.getModificationTime() > 0);
  }

  @Test
  public void testThrowExceptionWhenListStatusForNonExistPath() {
    FileSystem fs = getFileSystem();
    Path path = getContract().getTestPath();

    assertThrows(FileNotFoundException.class,
        () -> fs.listStatusIterator(new Path(path, "testListStatusForNonExistPath")),
        "Path doesn't exist");
  }

  @Test
  public void testPathStatNonexistentFile() {
    FileSystem fs = getFileSystem();
    // working dir does not exist.
    Path file = new Path(getContract().getTestPath(), this.methodName.getMethodName());
    assertThrows(FileNotFoundException.class, () -> fs.getFileStatus(file), "Path doesn't exist");
  }

  @Test
  public void testPathStatExistentFile() throws IOException {
    FileSystem fs = getFileSystem();
    Path file = new Path(getContract().getTestPath(), this.methodName.getMethodName());

    int size = 1 << 20;
    byte[] data = dataset(size, 'a', 'z');
    createFile(fs, file, true, data);
    FileStatus status = fs.getFileStatus(file);
    assertTrue(status.isFile());
    assertTrue(status.getModificationTime() > 0);
    assertEquals(size, status.getLen());
  }

  @Test
  public void testPathStatEmptyDirectory() throws IOException {
    FileSystem fs = getFileSystem();
    Path workingPath = new Path(getContract().getTestPath(), this.methodName.getMethodName());
    mkdirs(workingPath);

    FileStatus dirStatus = fs.getFileStatus(workingPath);
    assertTrue(dirStatus.isDirectory());
    assertTrue(dirStatus.getModificationTime() > 0);
    if (dirStatus instanceof RawFileStatus) {
      assertArrayEquals(Constants.MAGIC_CHECKSUM, ((RawFileStatus) dirStatus).checksum());
    }
  }

  @Test
  public void testPathStatWhenCreateSubDir() throws IOException {
    FileSystem fs = getFileSystem();
    Path workintPath = new Path(getContract().getTestPath(), this.methodName.getMethodName());
    // create sub directory directly.
    Path subDir = new Path(workintPath, UUIDUtils.random());
    mkdirs(subDir);
    assertTrue(fs.getFileStatus(subDir).isDirectory());

    // can get FileStatus of working dir.
    assertTrue(fs.getFileStatus(workintPath).isDirectory());
    // delete sub directory.
    fs.delete(subDir, true);
    // still cat get FileStatus of working dir.
    assertTrue(fs.getFileStatus(workintPath).isDirectory());
  }

  @Test
  public void testPathStatDirNotExistButSubFileExist() throws IOException {
    FileSystem fs = getFileSystem();
    // working dir does not exist.
    Path workintPath = new Path(getContract().getTestPath(), this.methodName.getMethodName());
    assertThrows(FileNotFoundException.class, () -> fs.getFileStatus(workintPath),
        "Path doesn't exist");

    // create sub file in working dir directly.
    Path file = workintPath.suffix('/' + UUIDUtils.random());
    touch(fs, file);

    // can get FileStatus of working dir.
    assertTrue(fs.getFileStatus(workintPath).isDirectory());

    // delete sub file, will create parent directory.
    fs.delete(file, false);
    assertTrue(fs.getFileStatus(workintPath).isDirectory());
  }
}
