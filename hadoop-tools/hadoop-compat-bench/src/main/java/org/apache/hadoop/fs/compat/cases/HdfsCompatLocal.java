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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.compat.common.*;

import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@HdfsCompatCaseGroup(name = "Local")
public class HdfsCompatLocal extends AbstractHdfsCompatCase {
  private static final int FILE_LEN = 128;
  private static final Random RANDOM = new Random();
  private LocalFileSystem localFs;
  private Path localBasePath;
  private Path localSrc;
  private Path localDst;
  private Path src;
  private Path dst;

  @HdfsCompatCaseSetUp
  public void setUp() throws IOException {
    localFs = FileSystem.getLocal(fs().getConf());
    localBasePath = localFs.makeQualified(getLocalPath());
  }

  @HdfsCompatCaseTearDown
  public void tearDown() {
    HdfsCompatUtil.deleteQuietly(localFs, localBasePath, true);
  }

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    final String unique = System.currentTimeMillis()
        + "_" + RANDOM.nextLong() + "/";
    this.localSrc = new Path(localBasePath, unique + "src");
    this.localDst = new Path(localBasePath, unique + "dst");
    this.src = new Path(getBasePath(), unique + "src");
    this.dst = new Path(getBasePath(), unique + "dst");
    HdfsCompatUtil.createFile(localFs, this.localSrc, FILE_LEN);
    HdfsCompatUtil.createFile(fs(), this.src, FILE_LEN);
  }

  @HdfsCompatCaseCleanup
  public void cleanup() {
    HdfsCompatUtil.deleteQuietly(fs(), this.src.getParent(), true);
    HdfsCompatUtil.deleteQuietly(localFs, this.localSrc.getParent(), true);
  }

  @HdfsCompatCase
  public void copyFromLocalFile() throws IOException {
    fs().copyFromLocalFile(localSrc, dst);
    assertTrue(localFs.exists(localSrc));
    assertTrue(fs().exists(dst));
  }

  @HdfsCompatCase
  public void moveFromLocalFile() throws IOException {
    fs().moveFromLocalFile(localSrc, dst);
    assertFalse(localFs.exists(localSrc));
    assertTrue(fs().exists(dst));
  }

  @HdfsCompatCase
  public void copyToLocalFile() throws IOException {
    fs().copyToLocalFile(src, localDst);
    assertTrue(fs().exists(src));
    assertTrue(localFs.exists(localDst));
  }

  @HdfsCompatCase
  public void moveToLocalFile() throws IOException {
    fs().moveToLocalFile(src, localDst);
    assertFalse(fs().exists(src));
    assertTrue(localFs.exists(localDst));
  }

  @HdfsCompatCase
  public void startLocalOutput() throws IOException {
    Path local = fs().startLocalOutput(dst, localDst);
    HdfsCompatUtil.createFile(localFs, local, 16);
    assertTrue(localFs.exists(local));
  }

  @HdfsCompatCase
  public void completeLocalOutput() throws IOException {
    Path local = fs().startLocalOutput(dst, localDst);
    HdfsCompatUtil.createFile(localFs, local, 16);
    fs().completeLocalOutput(dst, localDst);
    assertTrue(fs().exists(dst));
  }
}