/**
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
package org.apache.hadoop.fs.shell.find;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.apache.hadoop.fs.shell.find.TestHelper.*;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;

public class TestType {
  private FileSystem mockFs;

  private PathData item;
  private FileStatus fileStatus;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    Path path = new Path("/one/two/test");
    fileStatus = mock(FileStatus.class);
    when(fileStatus.isDirectory()).thenReturn(false);
    when(fileStatus.isSymlink()).thenReturn(false);
    when(fileStatus.isFile()).thenReturn(false);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    item = new PathData(path.toString(), mockFs.getConf());
  }

  // test path is a directory
  @Test
  public void testIsDirectory() throws IOException {
    Type type = new Type();
    addArgument(type, "d");
    type.setOptions(new FindOptions());
    type.prepare();

    when(fileStatus.isDirectory()).thenReturn(true);

    assertEquals(Result.PASS, type.apply(item, -1));
  }

  // test path is not a directory
  @Test
  public void testIsNotDirectory() throws IOException {
    Type type = new Type();
    addArgument(type, "d");
    type.setOptions(new FindOptions());
    type.prepare();

    when(fileStatus.isDirectory()).thenReturn(false);

    assertEquals(Result.FAIL, type.apply(item, -1));
  }

  // test path is a symlink
  @Test
  public void testIsSymlink() throws IOException {
    Type type = new Type();
    addArgument(type, "l");
    type.setOptions(new FindOptions());
    type.prepare();

    when(fileStatus.isSymlink()).thenReturn(true);

    assertEquals(Result.PASS, type.apply(item, -1));
  }

  // test path is not a symlink
  @Test
  public void testIsNotSymlink() throws IOException {
    Type type = new Type();
    addArgument(type, "l");
    type.setOptions(new FindOptions());
    type.prepare();

    when(fileStatus.isSymlink()).thenReturn(false);

    assertEquals(Result.FAIL, type.apply(item, -1));
  }

  // test path is a file
  @Test
  public void testIsFile() throws IOException {
    Type type = new Type();
    addArgument(type, "f");
    type.setOptions(new FindOptions());
    type.prepare();

    when(fileStatus.isFile()).thenReturn(true);

    assertEquals(Result.PASS, type.apply(item, -1));
  }

  // test path is not a file
  @Test
  public void testIsNotFile() throws IOException {
    Type type = new Type();
    addArgument(type, "f");
    type.setOptions(new FindOptions());
    type.prepare();

    when(fileStatus.isFile()).thenReturn(false);

    assertEquals(Result.FAIL, type.apply(item, -1));
  }

  // test an invalid file type throws an exception
  @Test
  public void testInvalidType() throws IOException {
    Type type = new Type();
    addArgument(type, "a");
    type.setOptions(new FindOptions());
    IOException expected = assertThrows(IOException.class, type::prepare);
    assertEquals("Invalid file type: a", expected.getMessage());
  }
}
