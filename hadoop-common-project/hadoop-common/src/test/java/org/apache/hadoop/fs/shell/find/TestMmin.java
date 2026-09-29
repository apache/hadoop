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

public class TestMmin {
  private static final long MIN = 60L * 1000L;
  private static final long NOW = new Date().getTime();

  private FileSystem mockFs;

  private PathData fourMinutes;
  private PathData fiveMinutes;
  private PathData fiveMinutesMinus;
  private PathData sixMinutes;
  private PathData sixMinutesMinus;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    fourMinutes = createPathData("fourMinutes", NOW - (4L * MIN));
    fiveMinutes = createPathData("fiveMinutes", NOW - (5L * MIN));
    fiveMinutesMinus = createPathData("fiveMinutesMinus", NOW - ((5L * MIN) - 1L));
    sixMinutes = createPathData("sixMinutes", (6L * MIN));
    sixMinutesMinus = createPathData("fiveMinutesPlus", NOW - ((6L * MIN) - 1L));
  }

  private PathData createPathData(String name, long mtime) throws IOException {
    Path path = new Path(name);
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getModificationTime()).thenReturn(mtime);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    return new PathData(name, mockFs.getConf());
  }

  // test an exact match
  @Test
  public void testExact() throws IOException {
    Mtime.Mmin mmin = new Mtime.Mmin();
    addArgument(mmin, "5");

    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    mmin.setOptions(options);
    mmin.prepare();

    assertEquals(Result.FAIL, mmin.apply(fourMinutes, -1));
    assertEquals(Result.FAIL, mmin.apply(fiveMinutesMinus, -1));
    assertEquals(Result.PASS, mmin.apply(fiveMinutes, -1));
    assertEquals(Result.PASS, mmin.apply(sixMinutesMinus, -1));
    assertEquals(Result.FAIL, mmin.apply(sixMinutes, -1));
  }

  // test a greater than match
  @Test
  public void testGreater() throws IOException {
    Mtime.Mmin mmin = new Mtime.Mmin();
    addArgument(mmin, "+5");

    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    mmin.setOptions(options);
    mmin.prepare();

    assertEquals(Result.FAIL, mmin.apply(fourMinutes, -1));
    assertEquals(Result.FAIL, mmin.apply(fiveMinutesMinus, -1));
    assertEquals(Result.FAIL, mmin.apply(fiveMinutes, -1));
    assertEquals(Result.FAIL, mmin.apply(sixMinutesMinus, -1));
    assertEquals(Result.PASS, mmin.apply(sixMinutes, -1));
  }

  // test a less than match
  @Test
  public void testLess() throws IOException {
    Mtime.Mmin mmin = new Mtime.Mmin();
    addArgument(mmin, "-5");

    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    mmin.setOptions(options);
    mmin.prepare();

    assertEquals(Result.PASS, mmin.apply(fourMinutes, -1));
    assertEquals(Result.PASS, mmin.apply(fiveMinutesMinus, -1));
    assertEquals(Result.FAIL, mmin.apply(fiveMinutes, -1));
    assertEquals(Result.FAIL, mmin.apply(sixMinutesMinus, -1));
    assertEquals(Result.FAIL, mmin.apply(sixMinutes, -1));
  }

}
