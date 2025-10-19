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

package org.apache.hadoop.fs.tosfs.ops;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TempFiles;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestRenameOp implements TestBaseOps {
  private static final String FILE_STORE_ROOT = TempFiles.newTempDir("TestRenameOp");

  private ObjectStorage storage;
  private ExecutorService renamePool;

  static Stream<Arguments> provideArguments() {
    assumeTrue(TestEnv.checkTestEnabled());

    List<Arguments> values = new ArrayList<>();
    List<ObjectStorage> storages = TestUtility.createTestObjectStorage(FILE_STORE_ROOT);
    for (ObjectStorage store : storages) {
      values.add(Arguments.of(store));
    }
    return values.stream();
  }

  @Override
  public ObjectStorage storage() {
    return storage;
  }

  private void setStorage(ObjectStorage storage) {
    this.storage = storage;
  }

  @BeforeEach
  public void prepare() {
    this.renamePool = ThreadPools.newWorkerPool("renamePool");
  }

  @AfterEach
  public void tearDown() {
    CommonUtils.runQuietly(() -> storage().deleteAll(""));
    CommonUtils.runQuietly(renamePool::shutdown);
  }

  @AfterAll
  public static void afterClass() {
    CommonUtils.runQuietly(() -> TempFiles.deleteDir(FILE_STORE_ROOT));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testRenameFileDirectly(ObjectStorage store) throws IOException {
    setStorage(store);

    Configuration conf = new Configuration();
    conf.setLong(ConfKeys.FS_MULTIPART_COPY_THRESHOLD.key(store.scheme()), 1L << 20);
    ExtendedRenameOp operation = new ExtendedRenameOp(conf, store, renamePool);

    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");

    int dataSize = 1024 * 1024;
    String filename = String.format("%sMB.txt", dataSize >> 20);
    Path srcFile = new Path(renameSrc, filename);
    Path dstFile = new Path(renameDest, filename);
    byte[] data = writeData(srcFile, dataSize);
    mkdir(renameDest);

    assertFileExist(srcFile);
    assertFileDoesNotExist(dstFile);
    assertDirExist(renameDest);

    operation.renameFile(srcFile, dstFile, data.length);
    assertFileDoesNotExist(srcFile);
    assertFileExist(dstFile);
    Map<String, List<Part>> uploadInfos = operation.uploadInfos;
    assertEquals(0, uploadInfos.size(),
        "use put method when rename file, upload info's size should be 0");

    try (InputStream in = store.get(ObjectUtils.pathToKey(dstFile)).stream()) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testRenameFileByUploadParts(ObjectStorage store) throws IOException {
    setStorage(store);

    assumeFalse(store.bucket().isDirectory());
    Configuration conf = new Configuration();
    conf.setLong(ConfKeys.FS_MULTIPART_COPY_THRESHOLD.key(store.scheme()), 1L << 20);
    ExtendedRenameOp operation = new ExtendedRenameOp(conf, store, renamePool);

    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");

    int dataSize = 10 * 1024 * 1024;
    String filename = String.format("%sMB.txt", dataSize >> 20);
    Path srcFile = new Path(renameSrc, filename);
    Path dstFile = new Path(renameDest, filename);
    byte[] data = writeData(srcFile, dataSize);
    mkdir(renameDest);

    assertFileExist(srcFile);
    assertFileDoesNotExist(dstFile);
    assertDirExist(renameDest);

    operation.renameFile(srcFile, dstFile, data.length);
    assertFileDoesNotExist(srcFile);
    assertFileExist(dstFile);
    Map<String, List<Part>> uploadInfos = operation.uploadInfos;
    assertTrue(uploadInfos.size() != 0,
        "use upload parts method when rename file, upload info's size should not be 0");
    List<Part> parts = uploadInfos.get(ObjectUtils.pathToKey(dstFile));
    assertNotNull(parts,
        "use upload parts method when rename file, upload info should not be null");
    assertTrue(parts.size() >= 2, "use upload parts method when rename file,"
        + " the num of upload parts should be greater than or equal to 2");
    long fileLength = parts.stream().mapToLong(Part::size).sum();
    assertEquals(dataSize, fileLength);

    try (InputStream in = store.get(ObjectUtils.pathToKey(dstFile)).stream()) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  private byte[] writeData(Path path, int size) {
    byte[] data = TestUtility.rand(size);
    touchFile(path, data);
    return data;
  }

  static class ExtendedRenameOp extends RenameOp {
    private Map<String, List<Part>> uploadInfos = Maps.newHashMap();

    ExtendedRenameOp(Configuration conf, ObjectStorage storage, ExecutorService pool) {
      super(conf, storage, pool);
    }

    @Override
    protected void finishUpload(String key, String uploadId, List<Part> uploadParts) {
      super.finishUpload(key, uploadId, uploadParts);
      if (!uploadInfos.isEmpty()) {
        uploadInfos.clear();
      }
      uploadInfos.put(key, uploadParts);
    }
  }
}
