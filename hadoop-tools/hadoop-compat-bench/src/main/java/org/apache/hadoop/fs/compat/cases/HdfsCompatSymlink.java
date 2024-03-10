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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.compat.common.*;
import org.junit.Assert;

import java.io.IOException;

@HdfsCompatCaseGroup(name = "Symlink")
public class HdfsCompatSymlink extends AbstractHdfsCompatCase {
  private static final int FILE_LEN = 128;
  private Path target = null;
  private Path link = null;

  @HdfsCompatCaseSetUp
  public void setUp() {
    FileSystem.enableSymlinks();
  }

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    this.target = makePath("target");
    this.link = new Path(this.target.getParent(), "link");
    HdfsCompatUtil.createFile(fs(), this.target, FILE_LEN);
    fs().createSymlink(this.target, this.link, true);
  }

  @HdfsCompatCaseCleanup
  public void cleanup() throws IOException {
    HdfsCompatUtil.deleteQuietly(fs(), this.link, true);
    HdfsCompatUtil.deleteQuietly(fs(), this.target, true);
  }

  @HdfsCompatCase
  public void createSymlink() throws IOException {
    Assert.assertTrue(fs().exists(link));
  }

  @HdfsCompatCase
  public void getFileLinkStatus() throws IOException {
    FileStatus linkStatus = fs().getFileLinkStatus(link);
    Assert.assertTrue(linkStatus.isSymlink());
    Assert.assertEquals(target.getName(), linkStatus.getSymlink().getName());
  }

  @HdfsCompatCase
  public void getLinkTarget() throws IOException {
    Path src = fs().getLinkTarget(link);
    Assert.assertEquals(target.getName(), src.getName());
  }
}