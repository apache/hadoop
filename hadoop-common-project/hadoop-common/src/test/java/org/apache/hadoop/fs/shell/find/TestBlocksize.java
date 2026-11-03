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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBlocksize {
  private FileSystem mockFs;

  private PathData one;
  private PathData two;
  private PathData three;
  private PathData four;
  private PathData five;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    one = createPathData("one", 1);
    two = createPathData("two", 2);
    three = createPathData("three", 3);
    four = createPathData("four", 4);
    five = createPathData("five", 5);
  }

  private PathData createPathData(String name, long blockSize) throws IOException {
    Path path = new Path(name);
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getBlockSize()).thenReturn(blockSize);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    return new PathData(name, mockFs.getConf());
  }

  // test exact match
  @Test
  public void applyEquals() throws IOException {
    Blocksize blocksize = new Blocksize();
    addArgument(blocksize, "3");
    blocksize.setOptions(new FindOptions());
    blocksize.prepare();

    assertEquals(Result.FAIL, blocksize.apply(one, -1));
    assertEquals(Result.FAIL, blocksize.apply(two, -1));
    assertEquals(Result.PASS, blocksize.apply(three, -1));
    assertEquals(Result.FAIL, blocksize.apply(four, -1));
    assertEquals(Result.FAIL, blocksize.apply(five, -1));
  }

  // test greater than match
  @Test
  public void applyGreaterThan() throws IOException {
    Blocksize blocksize = new Blocksize();
    addArgument(blocksize, "+3");
    blocksize.setOptions(new FindOptions());
    blocksize.prepare();

    assertEquals(Result.FAIL, blocksize.apply(one, -1));
    assertEquals(Result.FAIL, blocksize.apply(two, -1));
    assertEquals(Result.FAIL, blocksize.apply(three, -1));
    assertEquals(Result.PASS, blocksize.apply(four, -1));
    assertEquals(Result.PASS, blocksize.apply(five, -1));
  }

  // test less than match
  @Test
  public void applyLessThan() throws IOException {
    Blocksize blocksize = new Blocksize();
    addArgument(blocksize, "-3");
    blocksize.setOptions(new FindOptions());
    blocksize.prepare();

    assertEquals(Result.PASS, blocksize.apply(one, -1));
    assertEquals(Result.PASS, blocksize.apply(two, -1));
    assertEquals(Result.FAIL, blocksize.apply(three, -1));
    assertEquals(Result.FAIL, blocksize.apply(four, -1));
    assertEquals(Result.FAIL, blocksize.apply(five, -1));
  }
}
