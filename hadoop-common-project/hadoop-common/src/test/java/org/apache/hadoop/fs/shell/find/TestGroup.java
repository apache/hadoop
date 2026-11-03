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
import static org.apache.hadoop.fs.shell.find.TestHelper.*;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;

public class TestGroup {
  private FileSystem mockFs;
  private FileStatus fileStatus;
  private PathData item;
  private Group group;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    Path path = new Path("/one/two/test");
    fileStatus = mock(FileStatus.class);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    item = new PathData(path.toString(), mockFs.getConf());

    group = new Group();
    addArgument(group, "group");
    group.setOptions(new FindOptions());
    group.prepare();
  }

  // test a matching group
  @Test
  public void applyMatch() throws IOException {
    when(fileStatus.getGroup()).thenReturn("group");

    assertEquals(Result.PASS, group.apply(item, -1));
  }

  // test a non-matching group
  @Test
  public void applyNotMatch() throws IOException {
    when(fileStatus.getGroup()).thenReturn("notgroup");

    assertEquals(Result.FAIL, group.apply(item, -1));
  }

  // test a blank group
  @Test
  public void applyBlank() throws IOException {
    when(fileStatus.getGroup()).thenReturn("");

    assertEquals(Result.FAIL, group.apply(item, -1));
  }

  // test a null group
  @Test
  public void applyNull() throws IOException {
    when(fileStatus.getGroup()).thenReturn(null);

    assertEquals(Result.FAIL, group.apply(item, -1));
  }
}
