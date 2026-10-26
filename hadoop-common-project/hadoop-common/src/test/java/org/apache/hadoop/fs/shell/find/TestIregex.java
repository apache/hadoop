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

import static org.apache.hadoop.fs.shell.find.TestHelper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.PathData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIregex {
  private FileSystem mockFs;
  private Regex.Iregex regex;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();
  }

  private void setup(String pattern) throws IOException {
    regex = new Regex.Iregex();
    addArgument(regex, pattern);
    regex.setOptions(new FindOptions());
    regex.prepare();
  }

  // test a matching tail
  @Test
  public void applyPassTail() throws IOException {
    setup(".*name");
    PathData item = new PathData("/directory/path/name", mockFs.getConf());
    assertEquals(Result.PASS, regex.apply(item, -1));
  }

  // test a matching middle
  @Test
  public void applyPassMid() throws IOException {
    setup(".*path.*");
    PathData item = new PathData("/directory/path/name", mockFs.getConf());
    assertEquals(Result.PASS, regex.apply(item, -1));
  }

  // test a matching head
  @Test
  public void applyPassHead() throws IOException {
    setup("/dir.*");
    PathData item = new PathData("/directory/path/name", mockFs.getConf());
    assertEquals(Result.PASS, regex.apply(item, -1));
  }

  // test a non-matching regex
  @Test
  public void applyFail() throws IOException {
    setup(".*not.*");
    PathData item = new PathData("/directory/path/name", mockFs.getConf());
    assertEquals(Result.FAIL, regex.apply(item, -1));
  }

  // test a non-matching head
  @Test
  public void applyFailHead() throws IOException {
    setup("path.*");
    PathData item = new PathData("/directory/path/name", mockFs.getConf());
    assertEquals(Result.FAIL, regex.apply(item, -1));
  }

  // test a non-matching tail
  @Test
  public void applyFailTail() throws IOException {
    setup(".*path");
    PathData item = new PathData("/directory/path/name", mockFs.getConf());
    assertEquals(Result.FAIL, regex.apply(item, -1));
  }

  // test matching mixed case
  @Test
  public void applyMixedCase() throws IOException {
    setup(".*name");
    PathData item = new PathData("/directory/path/NaMe", mockFs.getConf());
    assertEquals(Result.PASS, regex.apply(item, -1));
  }
}
