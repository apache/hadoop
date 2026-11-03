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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAtime {
  private static final long DAY = 86400L * 1000L;
  private static final long NOW = new Date().getTime();

  private FileSystem mockFs;

  private PathData fourDays;
  private PathData fiveDays;
  private PathData fiveDaysMinus;
  private PathData sixDays;
  private PathData sixDaysMinus;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    fourDays = createPathData("fourDays", NOW - (4L * DAY));
    fiveDays = createPathData("fiveDays", NOW - (5L * DAY));
    fiveDaysMinus = createPathData("fiveDaysMinus", NOW - ((5L * DAY) - 1L));
    sixDays = createPathData("sixDays", (6L * DAY));
    sixDaysMinus = createPathData("sixDaysMinus", NOW - ((6L * DAY) - 1L));
  }

  private PathData createPathData(String name, long atime) throws IOException {
    Path path = new Path(name);
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getAccessTime()).thenReturn(atime);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    return new PathData(name, mockFs.getConf());
  }

  // test exact match
  @Test
  public void testExact() throws IOException {
    Atime atime = new Atime();
    addArgument(atime, "5");

    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    atime.setOptions(options);
    atime.prepare();

    assertEquals(Result.FAIL, atime.apply(fourDays, -1));
    assertEquals(Result.FAIL, atime.apply(fiveDaysMinus, -1));
    assertEquals(Result.PASS, atime.apply(fiveDays, -1));
    assertEquals(Result.PASS, atime.apply(sixDaysMinus, -1));
    assertEquals(Result.FAIL, atime.apply(sixDays, -1));
  }

  // test greater than match
  @Test
  public void testGreater() throws IOException {
    Atime atime = new Atime();
    addArgument(atime, "+5");

    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    atime.setOptions(options);
    atime.prepare();

    assertEquals(Result.FAIL, atime.apply(fourDays, -1));
    assertEquals(Result.FAIL, atime.apply(fiveDaysMinus, -1));
    assertEquals(Result.FAIL, atime.apply(fiveDays, -1));
    assertEquals(Result.FAIL, atime.apply(sixDaysMinus, -1));
    assertEquals(Result.PASS, atime.apply(sixDays, -1));
  }

  // test less than match
  @Test
  public void testLess() throws IOException {
    Atime atime = new Atime();
    addArgument(atime, "-5");

    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    atime.setOptions(options);
    atime.prepare();

    assertEquals(Result.PASS, atime.apply(fourDays, -1));
    assertEquals(Result.PASS, atime.apply(fiveDaysMinus, -1));
    assertEquals(Result.FAIL, atime.apply(fiveDays, -1));
    assertEquals(Result.FAIL, atime.apply(sixDaysMinus, -1));
    assertEquals(Result.FAIL, atime.apply(sixDays, -1));
  }
}
