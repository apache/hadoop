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

package org.apache.hadoop.fs.bos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the rename and create behaviour of {@link BaiduBosFileSystem}
 * on hierarchy (namespace) buckets, with the object store mocked so that
 * no BOS endpoint or credentials are needed.
 */
public class TestBaiduBosFileSystemHierarchy {

  private BosNativeFileSystemStore store;
  private BaiduBosFileSystem fs;

  @BeforeEach
  public void setUp() throws Exception {
    store = mock(BosNativeFileSystemStore.class);
    when(store.isHierarchy()).thenReturn(true);
    when(store.createFile(anyString(), any(Configuration.class)))
        .thenReturn(new ByteArrayOutputStream());

    fs = new BaiduBosFileSystem();
    fs.setConf(new Configuration());
    set("uri", URI.create("bos://bucket"));
    set("store", store);
    set("workingDir", new Path("bos://bucket/user/test"));
  }

  private void set(String name, Object value) throws Exception {
    Field field = BaiduBosFileSystem.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(fs, value);
  }

  private void file(String key) throws Exception {
    when(store.retrieveMetadata(key))
        .thenReturn(new FileMetadata(key, 10L, 1L, false));
  }

  private void dir(String key) throws Exception {
    when(store.retrieveMetadata(key))
        .thenReturn(new FileMetadata(key, 0L, 1L, true));
  }

  private void create(String path) throws Exception {
    fs.create(new Path(path), FsPermission.getFileDefault(), true,
        4096, (short) 1, 1024L, null).close();
  }

  @Test
  public void testCreateUnderFileThrowsParentNotDirectory()
      throws Exception {
    file("d/f");
    assertThrows(ParentNotDirectoryException.class,
        () -> create("/d/f/child"));
    verify(store, never()).createFile(anyString(),
        any(Configuration.class));
  }

  @Test
  public void testCreateUnderFileFurtherUpThrowsParentNotDirectory()
      throws Exception {
    file("d/f");
    assertThrows(ParentNotDirectoryException.class,
        () -> create("/d/f/x/y/child"));
    verify(store, never()).createFile(anyString(),
        any(Configuration.class));
  }

  @Test
  public void testCreateUnderExistingDirectory() throws Exception {
    dir("d");
    create("/d/child");
    verify(store).createFile(anyString(), any(Configuration.class));
  }

  @Test
  public void testCreateWithMissingParentsUnderDirectory()
      throws Exception {
    dir("d");
    create("/d/x/y/child");
    verify(store).createFile(anyString(), any(Configuration.class));
  }

  @Test
  public void testCreateAtBucketRoot() throws Exception {
    create("/child");
    verify(store).createFile(anyString(), any(Configuration.class));
  }

  @Test
  public void testFlatBucketDoesNotCheckAncestors() throws Exception {
    when(store.isHierarchy()).thenReturn(false);
    file("d/f");
    create("/d/f/child");
    verify(store, never()).retrieveMetadata("d/f");
    verify(store).createFile(anyString(), any(Configuration.class));
  }

  @Test
  public void testRenameMissingSourceReturnsFalse() throws Exception {
    assertFalse(fs.rename(new Path("/a"), new Path("/b")));
    verify(store, never()).rename(anyString(), anyString(), anyBoolean());
  }

  @Test
  public void testRenameToSelfIsNoOp() throws Exception {
    file("a");
    assertTrue(fs.rename(new Path("/a"), new Path("/a")));
    verify(store, never()).rename(anyString(), anyString(), anyBoolean());
  }

  @Test
  public void testRenameIntoOwnDescendantReturnsFalse() throws Exception {
    dir("a");
    assertFalse(fs.rename(new Path("/a"), new Path("/a/b/c")));
    verify(store, never()).rename(anyString(), anyString(), anyBoolean());
  }

  @Test
  public void testRenameFileToMissingPath() throws Exception {
    file("a");
    assertTrue(fs.rename(new Path("/a"), new Path("/b")));
    verify(store).rename("a", "b", true);
  }

  @Test
  public void testRenameIntoExistingDirectoryAppendsSourceName()
      throws Exception {
    file("a");
    dir("d");
    assertTrue(fs.rename(new Path("/a"), new Path("/d")));
    verify(store).rename("a", "d/a", true);
  }

  @Test
  public void testRenameDirectoryIntoExistingDirectory() throws Exception {
    dir("a");
    dir("d");
    assertTrue(fs.rename(new Path("/a"), new Path("/d")));
    verify(store).rename("a", "d/a", false);
  }

  @Test
  public void testRenameOntoNonEmptyDirectoryFails() throws Exception {
    dir("a");
    dir("d");
    dir("d/a");
    assertThrows(FileAlreadyExistsException.class,
        () -> fs.rename(new Path("/a"), new Path("/d")));
    verify(store, never()).rename(anyString(), anyString(), anyBoolean());
  }

  @Test
  public void testRenameFileOntoDirectoryContainingSameNameFails()
      throws Exception {
    file("a");
    dir("d");
    dir("d/a");
    assertThrows(FileAlreadyExistsException.class,
        () -> fs.rename(new Path("/a"), new Path("/d")));
    verify(store, never()).rename(anyString(), anyString(), anyBoolean());
  }

  @Test
  public void testMissingMetadataIsFileNotFound() {
    assertThrows(FileNotFoundException.class,
        () -> fs.getFileStatus(new Path("/nothing")));
  }
}
