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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPathData {
  protected Configuration conf;
  protected FileSystem fs;
  protected Path testDir;

  @Before
  public void initialize() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testDir = new Path(
        System.getProperty("test.build.data", "build/test/data") + "/testPD"
    );
    // don't want scheme on the path, just an absolute path
    testDir = new Path(fs.makeQualified(testDir).toUri().getPath());
    FileSystem.setDefaultUri(conf, fs.getUri());    
    fs.setWorkingDirectory(testDir);
    fs.mkdirs(new Path("d1"));
    fs.createNewFile(new Path("d1", "f1"));
    fs.createNewFile(new Path("d1", "f1.1"));
    fs.createNewFile(new Path("d1", "f2"));
    fs.mkdirs(new Path("d2"));
    fs.create(new Path("d2","f3"));
  }

  @After
  public void cleanup() throws Exception {
    fs.close();
  }

  @Test (timeout = 30000)
  public void testWithDirStringAndConf() throws Exception {
    String dirString = "d1";
    PathData item = new PathData(dirString, conf);
    checkPathData(dirString, item);

    // properly implementing symlink support in various commands will require
    // trailing slashes to be retained
    dirString = "d1/";
    item = new PathData(dirString, conf);
    checkPathData(dirString, item);
  }

  @Test (timeout = 30000)
  public void testUnqualifiedUriContents() throws Exception {
    String dirString = "d1";
    PathData item = new PathData(dirString, conf);
    PathData[] items = item.getDirectoryContents();
    assertEquals(
        sortedString("d1/f1", "d1/f1.1", "d1/f2"),
        sortedString(items)
    );
  }

  @Test (timeout = 30000)
  public void testQualifiedUriContents() throws Exception {
    String dirString = fs.makeQualified(new Path("d1")).toString();
    PathData item = new PathData(dirString, conf);
    PathData[] items = item.getDirectoryContents();
    assertEquals(
        sortedString(dirString+"/f1", dirString+"/f1.1", dirString+"/f2"),
        sortedString(items)
    );
  }

  @Test (timeout = 30000)
  public void testCwdContents() throws Exception {
    String dirString = Path.CUR_DIR;
    PathData item = new PathData(dirString, conf);
    PathData[] items = item.getDirectoryContents();
    assertEquals(
        sortedString("d1", "d2"),
        sortedString(items)
    );
  }

  @Test (timeout = 30000)
  public void testToFile() throws Exception {
    PathData item = new PathData(".", conf);
    assertEquals(new File(testDir.toString()), item.toFile());
    item = new PathData("d1/f1", conf);
    assertEquals(new File(testDir + "/d1/f1"), item.toFile());
    item = new PathData(testDir + "/d1/f1", conf);
    assertEquals(new File(testDir + "/d1/f1"), item.toFile());
  }

  @Test (timeout = 5000)
  public void testToFileRawWindowsPaths() throws Exception {
    if (!Path.WINDOWS) {
      return;
    }

    // Can we handle raw Windows paths? The files need not exist for
    // these tests to succeed.
    String[] winPaths = {
        "n:\\",
        "N:\\",
        "N:\\foo",
        "N:\\foo\\bar",
        "N:/",
        "N:/foo",
        "N:/foo/bar"
    };

    PathData item;

    for (String path : winPaths) {
      item = new PathData(path, conf);
      assertEquals(new File(path), item.toFile());
    }

    item = new PathData("foo\\bar", conf);
    assertEquals(new File(testDir + "\\foo\\bar"), item.toFile());
  }

  @Test (timeout = 5000)
  public void testInvalidWindowsPath() throws Exception {
    if (!Path.WINDOWS) {
      return;
    }

    // Verify that the following invalid paths are rejected.
    String [] winPaths = {
        "N:\\foo/bar"
    };

    for (String path : winPaths) {
      try {
        PathData item = new PathData(path, conf);
        fail("Did not throw for invalid path " + path);
      } catch (IOException ioe) {
      }
    }
  }

  @Test (timeout = 30000)
  public void testAbsoluteGlob() throws Exception {
    PathData[] items = PathData.expandAsGlob(testDir+"/d1/f1*", conf);
    assertEquals(
        sortedString(testDir+"/d1/f1", testDir+"/d1/f1.1"),
        sortedString(items)
    );
  }

  @Test (timeout = 30000)
  public void testRelativeGlob() throws Exception {
    PathData[] items = PathData.expandAsGlob("d1/f1*", conf);
    assertEquals(
        sortedString("d1/f1", "d1/f1.1"),
        sortedString(items)
    );
  }

  @Test (timeout = 30000)
  public void testRelativeGlobBack() throws Exception {
    fs.setWorkingDirectory(new Path("d1"));
    PathData[] items = PathData.expandAsGlob("../d2/*", conf);
    assertEquals(
        sortedString("../d2/f3"),
        sortedString(items)
    );
  }

  @Test (timeout = 30000)
  public void testWithStringAndConfForBuggyPath() throws Exception {
    String dirString = "file:///tmp";
    Path tmpDir = new Path(dirString);
    PathData item = new PathData(dirString, conf);
    // this may fail some day if Path is fixed to not crunch the uri
    // if the authority is null, however we need to test that the PathData
    // toString() returns the given string, while Path toString() does
    // the crunching
    assertEquals("file:/tmp", tmpDir.toString());
    checkPathData(dirString, item);
  }

  public void checkPathData(String dirString, PathData item) throws Exception {
    assertEquals("checking fs", fs, item.fs);
    assertEquals("checking string", dirString, item.toString());
    assertEquals("checking path",
        fs.makeQualified(new Path(item.toString())), item.path
    );
    assertTrue("checking exist", item.stat != null);
    assertTrue("checking isDir", item.stat.isDirectory());
  }
  
  /* junit does a lousy job of comparing arrays
   * if the array lengths differ, it just says that w/o showing contents
   * this sorts the paths, and builds a string of "i:<value>, ..." suitable
   * for a string compare
   */
  private static String sortedString(Object ... list) {
    String[] strings = new String[list.length];
    for (int i=0; i < list.length; i++) {
      strings[i] = String.valueOf(list[i]);
    }
    Arrays.sort(strings);
    
    StringBuilder result = new StringBuilder();
    for (int i=0; i < strings.length; i++) {
      if (result.length() > 0) {
        result.append(", ");
      }
      result.append(i+":<"+strings[i]+">");
    }
    return result.toString();
  }
  
  private static String sortedString(PathData ... items) {
    return sortedString((Object[])items);
  }
}
