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
package org.apache.hadoop.fs.shell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestPathData {
  protected static Configuration conf;
  protected static FileSystem fs;
  protected static String dirString;
  protected static Path dir;
  protected static PathData item;

  @BeforeClass
  public static void initialize() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf); 
  }

  @Test
  public void testWithFsAndPath() throws Exception {
    dirString = "/tmp";
    dir = new Path(dirString);
    item = new PathData(fs, dir);
    checkPathData();
  }

  @Test
  public void testWithStringAndConf() throws Exception {
    dirString = "/tmp";
    dir = new Path(dirString);
    item = new PathData(dirString, conf);
    checkPathData();
  }

  @Test
  public void testWithStringAndConfForBuggyPath() throws Exception {
    dirString = "file:///tmp";
    dir = new Path(dirString);
    item = new PathData(dirString, conf);
    // this may fail some day if Path is fixed to not crunch the uri
    // if the authority is null, however we need to test that the PathData
    // toString() returns the given string, while Path toString() does
    // the crunching
    assertEquals("file:/tmp", dir.toString());
    checkPathData();
  }

  public void checkPathData() throws Exception {
    assertEquals(fs, item.fs);
    assertEquals(dirString, item.toString());
    assertEquals(dir, item.path);
    assertTrue(item.stat != null);
    assertTrue(item.stat.isDirectory());
  }
}
