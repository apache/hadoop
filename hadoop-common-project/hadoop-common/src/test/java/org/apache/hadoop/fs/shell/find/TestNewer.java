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
import java.util.Date;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;

public class TestNewer {
  private static final long DAY = 86400L * 1000L;
  private static final long NOW = new Date().getTime();
  private FileSystem mockFs;
  private Newer newer;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    PathData comparison = createPathData("comparison.file", NOW - (5L * DAY));

    newer = new Newer();
    newer.setConf(mockFs.getConf());
    addArgument(newer, comparison.toString());
    newer.setOptions(new FindOptions());
    newer.prepare();
  }

  private PathData createPathData(String name, long mtime) throws IOException {
    Path path = new Path(name);
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getModificationTime()).thenReturn(mtime);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    return new PathData(name, mockFs.getConf());
  }

  // test a newer file
  @Test
  public void testNewer() throws IOException {
    PathData item = createPathData("/directory/path/newer.file", NOW - (4L * DAY));
    assertEquals(Result.PASS, newer.apply(item, -1));
  }

  // test an equal-time file
  @Test
  public void testEqual() throws IOException {
    PathData item = createPathData("/directory/path/equal.file", NOW - (5L * DAY));
    assertEquals(Result.FAIL, newer.apply(item, -1));
  }

  // test an older file
  @Test
  public void testOlder() throws IOException {
    PathData item = createPathData("/directory/path/older.file", NOW - (6L * DAY));
    assertEquals(Result.FAIL, newer.apply(item, -1));
  }
}
