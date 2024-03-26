/**
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
package org.apache.hadoop.fs.compat.cases;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.compat.common.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Assert;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Random;

@HdfsCompatCaseGroup(name = "File")
public class HdfsCompatFile extends AbstractHdfsCompatCase {
  private static final int FILE_LEN = 128;
  private static final long BLOCK_SIZE = 1048576;
  private static final short REPLICATION = 1;
  private static final Random RANDOM = new Random();
  private Path file = null;

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    this.file = makePath("file");
    HdfsCompatUtil.createFile(fs(), this.file, true,
        1024, FILE_LEN, BLOCK_SIZE, REPLICATION);
  }

  @HdfsCompatCaseCleanup
  public void cleanup() throws IOException {
    HdfsCompatUtil.deleteQuietly(fs(), this.file, true);
  }

  @HdfsCompatCase
  public void getFileStatus() throws IOException {
    FileStatus fileStatus = fs().getFileStatus(file);
    Assert.assertNotNull(fileStatus);
    Assert.assertEquals(file.getName(), fileStatus.getPath().getName());
  }

  @HdfsCompatCase
  public void exists() throws IOException {
    Assert.assertTrue(fs().exists(file));
  }

  @HdfsCompatCase
  public void isFile() throws IOException {
    Assert.assertTrue(fs().isFile(file));
  }

  @HdfsCompatCase
  public void getLength() throws IOException {
    Assert.assertEquals(FILE_LEN, fs().getLength(file));
  }

  @HdfsCompatCase(brief = "arbitrary blockSize")
  public void getBlockSize() throws IOException {
    Assert.assertEquals(BLOCK_SIZE, fs().getBlockSize(file));
  }

  @HdfsCompatCase
  public void renameFile() throws IOException {
    Path dst = new Path(file.toString() + "_rename_dst");
    fs().rename(file, dst);
    Assert.assertFalse(fs().exists(file));
    Assert.assertTrue(fs().exists(dst));
  }

  @HdfsCompatCase
  public void deleteFile() throws IOException {
    fs().delete(file, true);
    Assert.assertFalse(fs().exists(file));
  }

  @HdfsCompatCase
  public void deleteOnExit() throws IOException {
    FileSystem newFs = FileSystem.newInstance(fs().getUri(), fs().getConf());
    newFs.deleteOnExit(file);
    newFs.close();
    Assert.assertFalse(fs().exists(file));
  }

  @HdfsCompatCase
  public void cancelDeleteOnExit() throws IOException {
    FileSystem newFs = FileSystem.newInstance(fs().getUri(), fs().getConf());
    newFs.deleteOnExit(file);
    newFs.cancelDeleteOnExit(file);
    newFs.close();
    Assert.assertTrue(fs().exists(file));
  }

  @HdfsCompatCase
  public void truncate() throws IOException, InterruptedException {
    int newLen = RANDOM.nextInt(FILE_LEN);
    boolean finished = fs().truncate(file, newLen);
    while (!finished) {
      Thread.sleep(1000);
      finished = fs().truncate(file, newLen);
    }
    FileStatus fileStatus = fs().getFileStatus(file);
    Assert.assertEquals(newLen, fileStatus.getLen());
  }

  @HdfsCompatCase
  public void setOwner() throws Exception {
    final String owner = "test_" + RANDOM.nextInt(1024);
    final String group = "test_" + RANDOM.nextInt(1024);
    final String privileged = getPrivilegedUser();
    UserGroupInformation.createRemoteUser(privileged).doAs(
        (PrivilegedExceptionAction<Void>) () -> {
          FileSystem.newInstance(fs().getUri(), fs().getConf())
              .setOwner(file, owner, group);
          return null;
        }
    );
    FileStatus fileStatus = fs().getFileStatus(file);
    Assert.assertEquals(owner, fileStatus.getOwner());
    Assert.assertEquals(group, fileStatus.getGroup());
  }

  @HdfsCompatCase
  public void setTimes() throws IOException {
    final long atime = System.currentTimeMillis();
    final long mtime = atime - 1000;
    fs().setTimes(file, mtime, atime);
    FileStatus fileStatus = fs().getFileStatus(file);
    Assert.assertEquals(mtime, fileStatus.getModificationTime());
    Assert.assertEquals(atime, fileStatus.getAccessTime());
  }

  @HdfsCompatCase
  public void concat() throws IOException {
    final Path dir = makePath("dir");
    try {
      final Path src = new Path(dir, "src");
      final Path dst = new Path(dir, "dst");
      HdfsCompatUtil.createFile(fs(), src, 64);
      HdfsCompatUtil.createFile(fs(), dst, 16);
      fs().concat(dst, new Path[]{src});
      FileStatus fileStatus = fs().getFileStatus(dst);
      Assert.assertEquals(16 + 64, fileStatus.getLen());
    } finally {
      HdfsCompatUtil.deleteQuietly(fs(), dir, true);
    }
  }

  @HdfsCompatCase
  public void getFileChecksum() throws IOException {
    FileChecksum checksum = fs().getFileChecksum(file);
    Assert.assertNotNull(checksum);
    Assert.assertNotNull(checksum.getChecksumOpt());
    DataChecksum.Type type = checksum.getChecksumOpt().getChecksumType();
    Assert.assertNotEquals(DataChecksum.Type.NULL, type);
  }

  @HdfsCompatCase
  public void getFileBlockLocations() throws IOException {
    BlockLocation[] locations = fs().getFileBlockLocations(file, 0, FILE_LEN);
    Assert.assertTrue(locations.length >= 1);
    BlockLocation location = locations[0];
    Assert.assertTrue(location.getLength() > 0);
  }

  @HdfsCompatCase
  public void getReplication() throws IOException {
    Assert.assertEquals(REPLICATION, fs().getReplication(file));
  }

  @HdfsCompatCase(brief = "arbitrary replication")
  public void setReplication() throws IOException {
    fs().setReplication(this.file, (short) 2);
    Assert.assertEquals(2, fs().getReplication(this.file));
  }

  @HdfsCompatCase
  public void getPathHandle() throws IOException {
    FileStatus status = fs().getFileStatus(file);
    PathHandle handle = fs().getPathHandle(status, Options.HandleOpt.path());
    final int maxReadLen = Math.min(FILE_LEN, 4096);
    byte[] data = new byte[maxReadLen];
    try (FSDataInputStream in = fs().open(handle, 1024)) {
      in.readFully(data);
    }
  }

  @HdfsCompatCase
  public void open() throws IOException {
    FSDataInputStream in = null;
    try {
      in = fs().open(file);
      in.read();
    } finally {
      IOUtils.closeStream(in);
    }
  }

  @HdfsCompatCase
  public void openFile() throws Exception {
    FSDataInputStream in = null;
    try {
      FutureDataInputStreamBuilder builder = fs().openFile(file);
      in = builder.build().get();
    } finally {
      IOUtils.closeStream(in);
    }
  }

  @HdfsCompatCase
  public void access() throws IOException {
    fs().access(file, FsAction.READ);
  }

  @HdfsCompatCase
  public void setPermission() throws IOException {
    fs().setPermission(file, FsPermission.createImmutable((short) 511));
    try {
      fs().access(file, FsAction.ALL);
      Assert.fail("Should not have write permission");
    } catch (Throwable ignored) {
    }
  }
}