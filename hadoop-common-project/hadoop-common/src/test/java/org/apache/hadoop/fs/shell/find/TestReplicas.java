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

public class TestReplicas {
  private FileSystem mockFs;

  private PathData one;
  private PathData two;
  private PathData three;
  private PathData four;
  private PathData five;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    one = createPathData("one", (short) 1);
    two = createPathData("two", (short) 2);
    three = createPathData("three", (short) 3);
    four = createPathData("four", (short) 4);
    five = createPathData("five", (short) 5);
  }

  private PathData createPathData(String name, short replicas) throws IOException {
    Path path = new Path(name);
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getReplication()).thenReturn(replicas);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    return new PathData(name, mockFs.getConf());
  }

  // test an exact match
  @Test
  public void applyEquals() throws IOException {
    Replicas rep = new Replicas();
    addArgument(rep, "3");
    rep.setOptions(new FindOptions());
    rep.prepare();

    assertEquals(Result.FAIL, rep.apply(one, -1));
    assertEquals(Result.FAIL, rep.apply(two, -1));
    assertEquals(Result.PASS, rep.apply(three, -1));
    assertEquals(Result.FAIL, rep.apply(four, -1));
    assertEquals(Result.FAIL, rep.apply(five, -1));
  }

  // test a greater than match
  @Test
  public void applyGreaterThan() throws IOException {
    Replicas rep = new Replicas();
    addArgument(rep, "+3");
    rep.setOptions(new FindOptions());
    rep.prepare();

    assertEquals(Result.FAIL, rep.apply(one, -1));
    assertEquals(Result.FAIL, rep.apply(two, -1));
    assertEquals(Result.FAIL, rep.apply(three, -1));
    assertEquals(Result.PASS, rep.apply(four, -1));
    assertEquals(Result.PASS, rep.apply(five, -1));
  }

  // test a less than match
  @Test
  public void applyLessThan() throws IOException {
    Replicas rep = new Replicas();
    addArgument(rep, "-3");
    rep.setOptions(new FindOptions());
    rep.prepare();

    assertEquals(Result.PASS, rep.apply(one, -1));
    assertEquals(Result.PASS, rep.apply(two, -1));
    assertEquals(Result.FAIL, rep.apply(three, -1));
    assertEquals(Result.FAIL, rep.apply(four, -1));
    assertEquals(Result.FAIL, rep.apply(five, -1));
  }
}
