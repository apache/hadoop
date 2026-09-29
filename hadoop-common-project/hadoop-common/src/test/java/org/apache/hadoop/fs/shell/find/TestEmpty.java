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
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;

public class TestEmpty {
  private FileSystem mockFs;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();
  }

  private PathData createPathData(String name, boolean isEmpty, boolean isDir) throws IOException {
    Path path = new Path(name);
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.isDirectory()).thenReturn(isDir);
    when(fileStatus.getLen()).thenReturn(isEmpty ? 0L : 1L);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    when(mockFs.listStatus(eq(path))).thenReturn(
        isEmpty ? new FileStatus[0] : new FileStatus[] {new FileStatus()});
    return new PathData(name, mockFs.getConf());
  }

  // test with an empty file
  @Test
  public void applyEmptyFile() throws IOException {
    PathData item = createPathData("emptyFile", true, false);

    Empty empty = new Empty();
    empty.setOptions(new FindOptions());

    assertEquals(Result.PASS, empty.apply(item, -1));
  }

  // test with a non-empty file
  @Test
  public void applyNotEmptyFile() throws IOException {
    PathData item = createPathData("notEmptyFile", false, false);

    Empty empty = new Empty();
    empty.setOptions(new FindOptions());

    assertEquals(Result.FAIL, empty.apply(item, -1));
  }

  // test with an empty directory
  @Test
  public void applyEmptyDirectory() throws IOException {
    PathData item = createPathData("emptyDirectory", true, true);

    Empty empty = new Empty();
    empty.setOptions(new FindOptions());

    assertEquals(Result.PASS, empty.apply(item, -1));
  }

  // test with a non-empty directory
  @Test
  public void applyNotEmptyDirectory() throws IOException {
    PathData item = createPathData("notEmptyDirectory", false, true);

    Empty empty = new Empty();
    empty.setOptions(new FindOptions());

    assertEquals(Result.FAIL, empty.apply(item, -1));
  }
}
