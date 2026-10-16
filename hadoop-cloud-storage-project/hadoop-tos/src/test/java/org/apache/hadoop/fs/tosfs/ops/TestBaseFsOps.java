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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TestBaseFsOps implements TestBaseOps {

  private ObjectStorage storage;

  @Override
  public ObjectStorage storage() {
    return storage;
  }

  private void setStorage(ObjectStorage storage) {
    this.storage = storage;
  }

  @AfterEach
  public void tearDown() {
    CommonUtils.runQuietly(() -> storage.deleteAll(""));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testDeleteFile(ObjectStorage store, FsOps fsOps) throws IOException {
    setStorage(store);
    Path path = new Path("/a/b");
    touchFile(path, TestUtility.rand(8));
    assertFileExist(path);

    fsOps.deleteFile(path);
    assertFileDoesNotExist(path);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testDeleteEmptyDir(ObjectStorage store, FsOps fsOps) throws IOException {
    setStorage(store);
    Path path = new Path("/a/b/");
    mkdir(path);

    fsOps.deleteDir(path, false);
    assertDirDoesNotExist(path);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testDeleteNonEmptyDir(ObjectStorage store, FsOps fsOps) throws IOException {
    setStorage(store);
    Path dirPath = new Path("/a/b/");
    Path subDirPath = new Path("/a/b/c/");
    Path filePath = new Path("/a/b/file.txt");
    mkdir(dirPath);
    mkdir(subDirPath);
    touchFile(filePath, new byte[10]);

    assertThrows(PathIsNotEmptyDirectoryException.class, () -> fsOps.deleteDir(dirPath, false));
    assertDirExist(dirPath);
    assertDirExist(subDirPath);
    assertFileExist(filePath);

    fsOps.deleteDir(dirPath, true);
    assertDirDoesNotExist(dirPath);
    assertDirDoesNotExist(subDirPath);
    assertFileDoesNotExist(filePath);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testCreateDirRecursive(ObjectStorage store, FsOps fsOps) throws IOException {
    setStorage(store);
    Path path = new Path("/aa/bb/cc");
    String key = ObjectUtils.pathToKey(path, true);
    String parentKey = ObjectUtils.pathToKey(path.getParent(), true);
    String grandparents = ObjectUtils.pathToKey(path.getParent().getParent(), true);

    assertDirDoesNotExist(parentKey);
    assertDirDoesNotExist(grandparents);

    fsOps.mkdirs(path);
    assertDirExist(key);
    assertDirExist(parentKey);
    assertDirExist(grandparents);

    store.delete(key);
    assertDirExist(parentKey);
    assertDirExist(grandparents);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListEmptyDir(ObjectStorage store, FsOps fsOps) {
    setStorage(store);
    Path dir = path("testListEmptyDir");
    mkdir(dir);

    assertFalse(listDir(fsOps, dir, false).iterator().hasNext());
    assertFalse(listDir(fsOps, dir, true).iterator().hasNext());
    assertTrue(fsOps.isEmptyDirectory(dir));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListNonExistDir(ObjectStorage store, FsOps fsOps) {
    setStorage(store);
    Path dir = path("testListNonExistDir");
    assertDirDoesNotExist(dir);

    assertFalse(listDir(fsOps, dir, false).iterator().hasNext());
    assertFalse(listDir(fsOps, dir, false).iterator().hasNext());
    assertTrue(fsOps.isEmptyDirectory(dir));
  }

  private Iterable<RawFileStatus> listDir(FsOps fsOps, Path dir, boolean recursive) {
    return fsOps.listDir(dir, recursive, s -> true);
  }

  private Iterable<RawFileStatus> listFiles(FsOps fsOps, Path dir, boolean recursive) {
    return fsOps.listDir(dir, recursive, s -> !ObjectInfo.isDir(s));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListAFileViaListDir(ObjectStorage store, FsOps fsOps) {
    setStorage(store);
    Path file = new Path("testListFileViaListDir");
    touchFile(file, TestUtility.rand(8));
    assertFalse(listDir(fsOps, file, false).iterator().hasNext());
    assertFalse(listDir(fsOps, file, true).iterator().hasNext());

    Path nonExistFile = new Path("testListFileViaListDir-nonExist");
    assertFileDoesNotExist(nonExistFile);
    assertFalse(listDir(fsOps, nonExistFile, false).iterator().hasNext());
    assertFalse(listDir(fsOps, nonExistFile, true).iterator().hasNext());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListFiles(ObjectStorage store, FsOps fsOps) {
    setStorage(store);
    Path dir = path("testListEmptyFiles");
    mkdir(dir);

    assertFalse(listFiles(fsOps, dir, false).iterator().hasNext());
    assertFalse(listFiles(fsOps, dir, true).iterator().hasNext());

    mkdir(new Path(dir, "subDir"));
    assertFalse(listFiles(fsOps, dir, false).iterator().hasNext());
    assertFalse(listFiles(fsOps, dir, true).iterator().hasNext());

    RawFileStatus subDir = listDir(fsOps, dir, false).iterator().next();
    assertFalse(subDir.isFile());
    assertEquals("subDir", subDir.getPath().getName());

    ObjectInfo fileObj = touchFile(new Path(dir, "subFile"), TestUtility.rand(8));
    RawFileStatus subFile = listFiles(fsOps, dir, false).iterator().next();
    assertArrayEquals(fileObj.checksum(), subFile.checksum());
    assertTrue(subFile.isFile());

    assertFalse(fsOps.isEmptyDirectory(dir));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testRecursiveList(ObjectStorage store, FsOps fsOps) {
    setStorage(store);
    Path root = path("root");
    Path file1 = path("root", "file1");
    Path file2 = path("root", "afile2");
    Path dir1 = path("root", "dir1");
    Path file3 = path("root", "dir1", "file3");

    mkdir(root);
    mkdir(dir1);
    touchFile(file1, TestUtility.rand(8));
    touchFile(file2, TestUtility.rand(8));
    touchFile(file3, TestUtility.rand(8));

    // List result is in sorted lexicographical order if recursive is false
    Assertions.assertThat(listDir(fsOps, root, false))
        .hasSize(3)
        .extracting(f -> f.getPath().getName())
        .contains("afile2", "dir1", "file1");

    // List result is in sorted lexicographical order if recursive is false
    Assertions.assertThat(listFiles(fsOps, root, false))
        .hasSize(2)
        .extracting(f -> f.getPath().getName())
        .contains("afile2", "file1");

    // listDir with recursive=true doesn't guarantee the return result in a sorted order
    Assertions.assertThat(listDir(fsOps, root, true))
        .hasSize(4)
        .extracting(f -> f.getPath().getName())
        .containsExactlyInAnyOrder("afile2", "dir1", "file1", "file3");

    // listFiles with recursive=true doesn't guarantee the return result in a sorted order
    Assertions.assertThat(listFiles(fsOps, root, true))
        .hasSize(3)
        .extracting(f -> f.getPath().getName())
        .containsExactlyInAnyOrder("afile2", "file1", "file3");
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testRenameFile(ObjectStorage store, FsOps fsOps) throws IOException {
    setStorage(store);
    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");

    int dataSize = 1024 * 1024;
    String filename = String.format("%sMB.txt", dataSize >> 20);
    Path srcFile = new Path(renameSrc, filename);
    byte[] data = writeData(srcFile, dataSize);
    Path dstFile = new Path(renameDest, filename);

    // The dest file and dest parent don't exist.
    assertFileExist(srcFile);
    assertDirDoesNotExist(renameDest);
    assertFileDoesNotExist(dstFile);

    fsOps.renameFile(srcFile, dstFile, data.length);
    assertFileDoesNotExist(srcFile);
    assertDirExist(renameSrc);
    assertFileExist(dstFile);

    try (InputStream in = store.get(ObjectUtils.pathToKey(dstFile)).stream()) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testRenameDir(ObjectStorage store, FsOps fsOps) throws IOException {
    setStorage(store);
    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");

    mkdir(renameSrc);
    int dataSize = 1024 * 1024;
    String filename = String.format("%sMB.txt", dataSize >> 20);
    Path srcFile = new Path(renameSrc, filename);
    Path dstFile = new Path(renameDest, filename);
    byte[] data = writeData(srcFile, dataSize);

    assertFileExist(srcFile);
    assertFileDoesNotExist(dstFile);
    assertDirExist(renameSrc);
    assertDirDoesNotExist(renameDest);

    fsOps.renameDir(renameSrc, renameDest);
    assertFileDoesNotExist(srcFile);
    assertDirDoesNotExist(renameSrc);
    assertFileExist(dstFile);
    assertDirExist(renameDest);

    try (InputStream in = store.get(ObjectUtils.pathToKey(dstFile)).stream()) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  private byte[] writeData(Path path, int size) {
    byte[] data = TestUtility.rand(size);
    touchFile(path, data);
    return data;
  }
}
