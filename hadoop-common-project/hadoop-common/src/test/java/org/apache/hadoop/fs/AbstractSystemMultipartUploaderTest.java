/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractSystemMultipartUploaderTest {

  abstract FileSystem getFS() throws IOException;

  abstract Path getBaseTestPath();

  @Test
  public void testMultipartUpload() throws Exception {
    FileSystem fs = getFS();
    Path file = new Path(getBaseTestPath(), "some-file");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= 100; ++i) {
      String contents = "ThisIsPart" + i + "\n";
      sb.append(contents);
      int len = contents.getBytes().length;
      InputStream is = IOUtils.toInputStream(contents, "UTF-8");
      PartHandle partHandle = mpu.putPart(file, is, i, uploadHandle, len);
      partHandles.add(Pair.of(i, partHandle));
    }
    PathHandle fd = mpu.complete(file, partHandles, uploadHandle);
    byte[] fdData = IOUtils.toByteArray(fs.open(fd));
    byte[] fileData = IOUtils.toByteArray(fs.open(file));
    String readString = new String(fdData);
    assertEquals(sb.toString(), readString);
    assertArrayEquals(fdData, fileData);
  }

  @Test
  public void testMultipartUploadReverseOrder() throws Exception {
    FileSystem fs = getFS();
    Path file = new Path(getBaseTestPath(), "some-file");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= 100; ++i) {
      String contents = "ThisIsPart" + i + "\n";
      sb.append(contents);
    }
    for (int i = 100; i > 0; --i) {
      String contents = "ThisIsPart" + i + "\n";
      int len = contents.getBytes().length;
      InputStream is = IOUtils.toInputStream(contents, "UTF-8");
      PartHandle partHandle = mpu.putPart(file, is, i, uploadHandle, len);
      partHandles.add(Pair.of(i, partHandle));
    }
    PathHandle fd = mpu.complete(file, partHandles, uploadHandle);
    byte[] fdData = IOUtils.toByteArray(fs.open(fd));
    byte[] fileData = IOUtils.toByteArray(fs.open(file));
    String readString = new String(fdData);
    assertEquals(sb.toString(), readString);
    assertArrayEquals(fdData, fileData);
  }

  @Test
  public void testMultipartUploadReverseOrderNoNContiguousPartNumbers()
      throws Exception {
    FileSystem fs = getFS();
    Path file = new Path(getBaseTestPath(), "some-file");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    for (int i = 2; i <= 200; i += 2) {
      String contents = "ThisIsPart" + i + "\n";
      sb.append(contents);
    }
    for (int i = 200; i > 0; i -= 2) {
      String contents = "ThisIsPart" + i + "\n";
      int len = contents.getBytes().length;
      InputStream is = IOUtils.toInputStream(contents, "UTF-8");
      PartHandle partHandle = mpu.putPart(file, is, i, uploadHandle, len);
      partHandles.add(Pair.of(i, partHandle));
    }
    PathHandle fd = mpu.complete(file, partHandles, uploadHandle);
    byte[] fdData = IOUtils.toByteArray(fs.open(fd));
    byte[] fileData = IOUtils.toByteArray(fs.open(file));
    String readString = new String(fdData);
    assertEquals(sb.toString(), readString);
    assertArrayEquals(fdData, fileData);
  }

  @Test
  public void testMultipartUploadAbort() throws Exception {
    FileSystem fs = getFS();
    Path file = new Path(getBaseTestPath(), "some-file");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    for (int i = 100; i >= 50; --i) {
      String contents = "ThisIsPart" + i + "\n";
      int len = contents.getBytes().length;
      InputStream is = IOUtils.toInputStream(contents, "UTF-8");
      PartHandle partHandle = mpu.putPart(file, is, i, uploadHandle, len);
    }
    mpu.abort(file, uploadHandle);

    String contents = "ThisIsPart49\n";
    int len = contents.getBytes().length;
    InputStream is = IOUtils.toInputStream(contents, "UTF-8");

    try {
      mpu.putPart(file, is, 49, uploadHandle, len);
      fail("putPart should have thrown an exception");
    } catch (IOException ok) {
      // ignore
    }
  }
}
