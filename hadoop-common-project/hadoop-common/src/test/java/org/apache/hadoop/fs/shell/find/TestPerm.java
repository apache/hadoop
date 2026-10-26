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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathData;

public class TestPerm {
  private FileSystem mockFs;

  private PathData mode777;
  private PathData mode700;
  private PathData mode444;
  private PathData mode730;
  private PathData mode740;
  private PathData mode123;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();

    mode777 = createPathData("rwxrwxrwx", "-rwxrwxrwx");
    mode700 = createPathData("rwx______", "-rwx------");
    mode444 = createPathData("r__r__r__", "-r--r--r--");
    mode730 = createPathData("rwx_wx___", "-rwx-wx---");
    mode740 = createPathData("rwxr_____", "-rwxr-----");
    mode123 = createPathData("__x_w__wx", "---x-w--wx");
  }

  private PathData createPathData(String name, String permission) throws IOException {
    Path path = new Path(name);
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getPermission()).thenReturn(FsPermission.valueOf(permission));
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStatus);
    return new PathData(path.toString(), mockFs.getConf());
  }

  // test an exact match of an octal mode
  @Test
  public void applyOctalExact() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "123");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.FAIL, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.FAIL, perm.apply(mode444, -1));
    assertEquals(Result.FAIL, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.PASS, perm.apply(mode123, -1));
  }

  // test a masked match of an octal mode
  @Test
  public void applyOctalMask() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "-123");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.PASS, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.FAIL, perm.apply(mode444, -1));
    assertEquals(Result.FAIL, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.PASS, perm.apply(mode123, -1));
  }

  // test an exact match of a symbolic mode
  @Test
  public void applySymbolicExact() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "u=x,g=w,o=wx");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.FAIL, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.FAIL, perm.apply(mode444, -1));
    assertEquals(Result.FAIL, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.PASS, perm.apply(mode123, -1));
  }

  // test a masked match of a symbolic mode
  @Test
  public void applySymbolicMask() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "-u=x,g=w,o=wx");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.PASS, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.FAIL, perm.apply(mode444, -1));
    assertEquals(Result.FAIL, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.PASS, perm.apply(mode123, -1));
  }

  // test a complex symbolic mode
  @Test
  public void applySymbolicMultipleOperator() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "u=rwx,g+x+w");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.FAIL, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.FAIL, perm.apply(mode444, -1));
    assertEquals(Result.PASS, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.FAIL, perm.apply(mode123, -1));
  }

  // test a complex symbolic mode
  @Test
  public void applySymbolicComplex() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "u=xrw,g=x,o=wx,u-rw,g+w,g-x");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.FAIL, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.FAIL, perm.apply(mode444, -1));
    assertEquals(Result.FAIL, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.PASS, perm.apply(mode123, -1));
  }

  // test an all symbolic mode
  @Test
  public void applyAllSymbolic() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "a=rwx");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.PASS, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.FAIL, perm.apply(mode444, -1));
    assertEquals(Result.FAIL, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.FAIL, perm.apply(mode123, -1));
  }

  // test a masked match of a all symbolic mode
  @Test
  public void applyAllSymbolicMask() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "-a=r");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.PASS, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.PASS, perm.apply(mode444, -1));
    assertEquals(Result.FAIL, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.FAIL, perm.apply(mode123, -1));
  }

  // test a complex all symbolic mode
  @Test
  public void applyAllSymbolicComplex() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "a=rwx,u-rw,g-rx,g+w,o-r");
    perm.setOptions(new FindOptions());
    perm.prepare();

    assertEquals(Result.FAIL, perm.apply(mode777, -1));
    assertEquals(Result.FAIL, perm.apply(mode700, -1));
    assertEquals(Result.FAIL, perm.apply(mode444, -1));
    assertEquals(Result.FAIL, perm.apply(mode730, -1));
    assertEquals(Result.FAIL, perm.apply(mode740, -1));
    assertEquals(Result.PASS, perm.apply(mode123, -1));
  }

  // test an invalid octal perm throws an exception
  @ParameterizedTest
  @ValueSource(strings = {"888", "1888"})
  public void testInvalidOctalPerm(String argument) throws IOException {
    Perm perm = new Perm();
    addArgument(perm, argument);
    perm.setOptions(new FindOptions());
    IllegalArgumentException expected = assertThrows(IllegalArgumentException.class, perm::prepare);
    assertEquals(argument, expected.getMessage());
  }

  // test an invalid perm throws an exception
  @ParameterizedTest
  @ValueSource(strings = {"t=x,g=w,o=wx", "u^x,g=w,o=wx", "u", "u=v,g=w,o=wx", "u=rwx,g"})
  public void testInvalidPerm(String argument) throws IOException {
    Perm perm = new Perm();
    addArgument(perm, argument);
    perm.setOptions(new FindOptions());
    IllegalArgumentException expected = assertThrows(IllegalArgumentException.class, perm::prepare);
    assertEquals("Invalid mode: " + argument, expected.getMessage());
  }

  // test an invalid empty argument throws an exception
  @Test
  public void testInvalidEmptyArgument() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "");
    perm.setOptions(new FindOptions());
    IllegalArgumentException expected = assertThrows(IllegalArgumentException.class, perm::prepare);
    assertEquals("Invalid empty argument", expected.getMessage());
  }
}
