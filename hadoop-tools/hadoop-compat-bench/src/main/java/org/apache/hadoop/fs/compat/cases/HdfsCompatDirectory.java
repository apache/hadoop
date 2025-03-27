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
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@HdfsCompatCaseGroup(name = "Directory")
public class HdfsCompatDirectory extends AbstractHdfsCompatCase {
  private static final int FILE_LEN = 128;
  private Path dir = null;
  private Path file = null;

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    this.dir = makePath("dir");
    this.file = new Path(this.dir, "file");
    HdfsCompatUtil.createFile(fs(), file, FILE_LEN);
  }

  @HdfsCompatCaseCleanup
  public void cleanup() throws IOException {
    HdfsCompatUtil.deleteQuietly(fs(), this.dir, true);
  }

  @HdfsCompatCase
  public void isDirectory() throws IOException {
    Assert.assertTrue(fs().isDirectory(dir));
  }

  @HdfsCompatCase
  public void listStatus() throws IOException {
    FileStatus[] files = fs().listStatus(dir);
    Assert.assertNotNull(files);
    Assert.assertEquals(1, files.length);
    Assert.assertEquals(file.getName(), files[0].getPath().getName());
  }

  @HdfsCompatCase
  public void globStatus() throws IOException {
    FileStatus[] files = fs().globStatus(new Path(dir, "*ile"));
    Assert.assertNotNull(files);
    Assert.assertEquals(1, files.length);
    Assert.assertEquals(file.getName(), files[0].getPath().getName());
  }

  @HdfsCompatCase
  public void listLocatedStatus() throws IOException {
    RemoteIterator<LocatedFileStatus> locatedFileStatuses =
        fs().listLocatedStatus(dir);
    Assert.assertNotNull(locatedFileStatuses);
    List<LocatedFileStatus> files = new ArrayList<>();
    while (locatedFileStatuses.hasNext()) {
      files.add(locatedFileStatuses.next());
    }
    Assert.assertEquals(1, files.size());
    LocatedFileStatus fileStatus = files.get(0);
    Assert.assertEquals(file.getName(), fileStatus.getPath().getName());
  }

  @HdfsCompatCase
  public void listStatusIterator() throws IOException {
    RemoteIterator<FileStatus> fileStatuses = fs().listStatusIterator(dir);
    Assert.assertNotNull(fileStatuses);
    List<FileStatus> files = new ArrayList<>();
    while (fileStatuses.hasNext()) {
      files.add(fileStatuses.next());
    }
    Assert.assertEquals(1, files.size());
    FileStatus fileStatus = files.get(0);
    Assert.assertEquals(file.getName(), fileStatus.getPath().getName());
  }

  @HdfsCompatCase
  public void listFiles() throws IOException {
    RemoteIterator<LocatedFileStatus> iter = fs().listFiles(dir, true);
    Assert.assertNotNull(iter);
    List<LocatedFileStatus> files = new ArrayList<>();
    while (iter.hasNext()) {
      files.add(iter.next());
    }
    Assert.assertEquals(1, files.size());
  }

  @HdfsCompatCase
  public void listCorruptFileBlocks() throws IOException {
    RemoteIterator<Path> iter = fs().listCorruptFileBlocks(dir);
    Assert.assertNotNull(iter);
    Assert.assertFalse(iter.hasNext());  // No corrupted file
  }

  @HdfsCompatCase
  public void getContentSummary() throws IOException {
    ContentSummary summary = fs().getContentSummary(dir);
    Assert.assertEquals(1, summary.getFileCount());
    Assert.assertEquals(1, summary.getDirectoryCount());
    Assert.assertEquals(FILE_LEN, summary.getLength());
  }

  @HdfsCompatCase
  public void getUsed() throws IOException {
    long used = fs().getUsed(dir);
    Assert.assertTrue(used >= FILE_LEN);
  }

  @HdfsCompatCase
  public void getQuotaUsage() throws IOException {
    QuotaUsage usage = fs().getQuotaUsage(dir);
    Assert.assertEquals(2, usage.getFileAndDirectoryCount());
  }

  @HdfsCompatCase
  public void setQuota() throws IOException {
    fs().setQuota(dir, 1048576L, 1073741824L);
    QuotaUsage usage = fs().getQuotaUsage(dir);
    Assert.assertEquals(1048576L, usage.getQuota());
  }

  @HdfsCompatCase
  public void setQuotaByStorageType() throws IOException {
    fs().setQuotaByStorageType(dir, StorageType.DISK, 1048576L);
    QuotaUsage usage = fs().getQuotaUsage(dir);
    Assert.assertEquals(1048576L, usage.getTypeQuota(StorageType.DISK));
  }
}