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
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

@HdfsCompatCaseGroup(name = "Create")
public class HdfsCompatCreate extends AbstractHdfsCompatCase {
  private Path path;

  @HdfsCompatCasePrepare
  public void prepare() {
    this.path = makePath("path");
  }

  @HdfsCompatCaseCleanup
  public void cleanup() {
    HdfsCompatUtil.deleteQuietly(fs(), this.path, true);
  }

  @HdfsCompatCase
  public void mkdirs() throws IOException {
    fs().mkdirs(path);
    Assert.assertTrue(fs().exists(path));
  }

  @HdfsCompatCase
  public void create() throws IOException {
    FSDataOutputStream out = null;
    try {
      out = fs().create(path, true);
      Assert.assertTrue(fs().exists(path));
    } finally {
      IOUtils.closeStream(out);
    }
  }

  @HdfsCompatCase
  public void createNonRecursive() {
    Path file = new Path(path, "file-no-parent");
    try {
      fs().createNonRecursive(file, true, 1024, (short) 1, 1048576, null);
      Assert.fail("Should fail since parent does not exist");
    } catch (IOException ignored) {
    }
  }

  @HdfsCompatCase
  public void createNewFile() throws IOException {
    HdfsCompatUtil.createFile(fs(), path, 0);
    Assert.assertFalse(fs().createNewFile(path));
  }

  @HdfsCompatCase
  public void append() throws IOException {
    HdfsCompatUtil.createFile(fs(), path, 128);
    FSDataOutputStream out = null;
    byte[] data = new byte[64];
    try {
      out = fs().append(path);
      out.write(data);
      out.close();
      out = null;
      FileStatus fileStatus = fs().getFileStatus(path);
      Assert.assertEquals(128 + 64, fileStatus.getLen());
    } finally {
      IOUtils.closeStream(out);
    }
  }

  @HdfsCompatCase
  public void createFile() throws IOException {
    FSDataOutputStream out = null;
    fs().mkdirs(path);
    final Path file = new Path(path, "file");
    try {
      FSDataOutputStreamBuilder builder = fs().createFile(file);
      out = builder.blockSize(1048576 * 2).build();
      out.write("Hello World!".getBytes(StandardCharsets.UTF_8));
      out.close();
      out = null;
      Assert.assertTrue(fs().exists(file));
    } finally {
      IOUtils.closeStream(out);
    }
  }

  @HdfsCompatCase
  public void appendFile() throws IOException {
    HdfsCompatUtil.createFile(fs(), path, 128);
    FSDataOutputStream out = null;
    byte[] data = new byte[64];
    try {
      FSDataOutputStreamBuilder builder = fs().appendFile(path);
      out = builder.build();
      out.write(data);
      out.close();
      out = null;
      FileStatus fileStatus = fs().getFileStatus(path);
      Assert.assertEquals(128 + 64, fileStatus.getLen());
    } finally {
      IOUtils.closeStream(out);
    }
  }

  @HdfsCompatCase
  public void createMultipartUploader() throws Exception {
    MultipartUploader mpu = null;
    UploadHandle handle = null;
    try {
      MultipartUploaderBuilder builder = fs().createMultipartUploader(path);
      final Path file = fs().makeQualified(new Path(path, "file"));
      mpu = builder.blockSize(1048576).build();
      CompletableFuture<UploadHandle> future = mpu.startUpload(file);
      handle = future.get();
    } finally {
      if (mpu != null) {
        if (handle != null) {
          try {
            mpu.abort(handle, path);
          } catch (Throwable ignored) {
          }
        }
        try {
          mpu.abortUploadsUnderPath(path);
        } catch (Throwable ignored) {
        }
      }
    }
  }
}