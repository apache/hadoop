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

public class TestSize {
  private FileSystem mockFs;

  private PathData fiveBlocks;
  private PathData fourBlocks;
  private PathData sixBlocks;
  private PathData fiveBlocksMinus;
  private PathData fiveBlocksPlus;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    fiveBlocks = createPathData("fiveBlocks", 5L * 512L);
    sixBlocks = createPathData("sixBlocks", 6L * 512L);
    fourBlocks = createPathData("fourBlocks", 4L * 512L);
    fiveBlocksPlus = createPathData("fiveBlocksPlus", (5L * 512L) + 511L);
    fiveBlocksMinus = createPathData("fiveBlocksMinus", (5L * 512L) - 1L);
  }

  private PathData createPathData(String name, long length) throws IOException {
    Path path = new Path(name);
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getLen()).thenReturn(length);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    return new PathData(name, mockFs.getConf());
  }

  // test exact match in blocks
  @Test
  public void applyEqualsBlock() throws IOException {
    Size size = new Size();
    addArgument(size, "5");
    size.setOptions(new FindOptions());
    size.prepare();

    assertEquals(Result.PASS, size.apply(fiveBlocks, -1));
    assertEquals(Result.FAIL, size.apply(sixBlocks, -1));
    assertEquals(Result.FAIL, size.apply(fourBlocks, -1));
    assertEquals(Result.FAIL, size.apply(fiveBlocksPlus, -1));
    assertEquals(Result.PASS, size.apply(fiveBlocksMinus, -1));
  }

  // test greater than match in blocks
  @Test
  public void applyGreaterThanBlock() throws IOException {
    Size size = new Size();
    addArgument(size, "+5");
    size.setOptions(new FindOptions());
    size.prepare();

    assertEquals(Result.FAIL, size.apply(fiveBlocks, -1));
    assertEquals(Result.PASS, size.apply(sixBlocks, -1));
    assertEquals(Result.FAIL, size.apply(fourBlocks, -1));
    assertEquals(Result.PASS, size.apply(fiveBlocksPlus, -1));
    assertEquals(Result.FAIL, size.apply(fiveBlocksMinus, -1));
  }

  // test less than match in blocks
  @Test
  public void applyLessThanBlock() throws IOException {
    Size size = new Size();
    addArgument(size, "-5");
    size.setOptions(new FindOptions());
    size.prepare();

    assertEquals(Result.FAIL, size.apply(fiveBlocks, -1));
    assertEquals(Result.FAIL, size.apply(sixBlocks, -1));
    assertEquals(Result.PASS, size.apply(fourBlocks, -1));
    assertEquals(Result.FAIL, size.apply(fiveBlocksPlus, -1));
    assertEquals(Result.FAIL, size.apply(fiveBlocksMinus, -1));
  }

  // test exact match in bytes
  @Test
  public void applyEqualsBytes() throws IOException {
    Size size = new Size();
    addArgument(size, (5 * 512) + "c");
    size.setOptions(new FindOptions());
    size.prepare();

    assertEquals(Result.PASS, size.apply(fiveBlocks, -1));
    assertEquals(Result.FAIL, size.apply(sixBlocks, -1));
    assertEquals(Result.FAIL, size.apply(fourBlocks, -1));
    assertEquals(Result.FAIL, size.apply(fiveBlocksPlus, -1));
    assertEquals(Result.FAIL, size.apply(fiveBlocksMinus, -1));
  }

  // test greater than match in bytes
  @Test
  public void applyGreaterThanBytes() throws IOException {
    Size size = new Size();
    addArgument(size, "+" + (5 * 512) + "c");
    size.setOptions(new FindOptions());
    size.prepare();

    assertEquals(Result.FAIL, size.apply(fiveBlocks, -1));
    assertEquals(Result.PASS, size.apply(sixBlocks, -1));
    assertEquals(Result.FAIL, size.apply(fourBlocks, -1));
    assertEquals(Result.PASS, size.apply(fiveBlocksPlus, -1));
    assertEquals(Result.FAIL, size.apply(fiveBlocksMinus, -1));
  }

  // test less than match in bytes
  @Test
  public void applyLessThanBytes() throws IOException {
    Size size = new Size();
    addArgument(size, "-" + (5 * 512) + "c");
    size.setOptions(new FindOptions());
    size.prepare();

    assertEquals(Result.FAIL, size.apply(fiveBlocks, -1));
    assertEquals(Result.FAIL, size.apply(sixBlocks, -1));
    assertEquals(Result.PASS, size.apply(fourBlocks, -1));
    assertEquals(Result.FAIL, size.apply(fiveBlocksPlus, -1));
    assertEquals(Result.PASS, size.apply(fiveBlocksMinus, -1));
  }
}
