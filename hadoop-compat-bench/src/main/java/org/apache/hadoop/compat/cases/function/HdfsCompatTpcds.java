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
package org.apache.hadoop.compat.cases.function;

import org.apache.hadoop.compat.*;
import org.apache.hadoop.fs.*;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@HdfsCompatCaseGroup(name = "TPCDS")
public class HdfsCompatTpcds extends AbstractHdfsCompatCase {
  private static final int fileLen = 8;
  private static final Random random = new Random();
  private Path path = null;

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    path = makePath("path");
  }

  @HdfsCompatCaseCleanup
  public void cleanup() throws IOException {
    HdfsCompatUtil.deleteQuietly(fs(), path, true);
  }

  @HdfsCompatCase
  public void open() throws IOException {
    HdfsCompatUtil.createFile(fs(), path, fileLen);
    byte[] data = new byte[fileLen];
    try (FSDataInputStream in = fs().open(path)) {
      in.readFully(data);
    }
  }

  @HdfsCompatCase
  public void create() throws IOException {
    byte[] data = new byte[fileLen];
    random.nextBytes(data);
    try (FSDataOutputStream out = fs().create(path, true)) {
      out.write(data);
    }
  }

  @HdfsCompatCase
  public void mkdirs() throws IOException {
    Assert.assertTrue(fs().mkdirs(path));
  }

  @HdfsCompatCase
  public void getFileStatus() throws IOException {
    HdfsCompatUtil.createFile(fs(), path, fileLen);
    FileStatus fileStatus = fs().getFileStatus(path);
    Assert.assertEquals(fileLen, fileStatus.getLen());
  }

  @HdfsCompatCase
  public void listStatus() throws IOException {
    HdfsCompatUtil.createFile(fs(), new Path(path, "file"), fileLen);
    FileStatus[] files = fs().listStatus(path);
    Assert.assertEquals(1, files.length);
    Assert.assertEquals(fileLen, files[0].getLen());
  }

  @HdfsCompatCase
  public void listLocatedStatus() throws IOException {
    HdfsCompatUtil.createFile(fs(), new Path(path, "file"), fileLen);
    RemoteIterator<LocatedFileStatus> it = fs().listLocatedStatus(path);
    List<LocatedFileStatus> files = new ArrayList<>();
    while (it.hasNext()) {
      files.add(it.next());
    }
    Assert.assertEquals(1, files.size());
    Assert.assertEquals(fileLen, files.get(0).getLen());
  }

  @HdfsCompatCase
  public void rename() throws IOException {
    HdfsCompatUtil.createFile(fs(), new Path(path, "file"), fileLen);
    fs().rename(path, new Path(path.getParent(), path.getName() + "_dst"));
  }

  @HdfsCompatCase
  public void delete() throws IOException {
    HdfsCompatUtil.createFile(fs(), new Path(path, "file"), fileLen);
    fs().delete(path, true);
  }

  @HdfsCompatCase
  public void getServerDefaults() throws IOException {
    Assert.assertNotNull(fs().getServerDefaults(path));
  }

  @HdfsCompatCase
  public void getTrashRoot() throws IOException {
    Assert.assertNotNull(fs().getTrashRoot(path));
  }

  @HdfsCompatCase
  public void makeQualified() throws IOException {
    Assert.assertNotNull(fs().makeQualified(path));
  }
}