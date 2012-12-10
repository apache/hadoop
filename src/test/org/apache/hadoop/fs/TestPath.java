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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import junit.framework.TestCase;

public class TestPath extends TestCase {
  public void testToString() {
    toStringTest("/");
    toStringTest("/foo");
    toStringTest("/foo/bar");
    toStringTest("foo");
    toStringTest("foo/bar");
    toStringTest("/foo/bar#boo");
    toStringTest("foo/bar#boo");
    boolean emptyException = false;
    try {
      toStringTest("");
    } catch (IllegalArgumentException e) {
      // expect to receive an IllegalArgumentException
      emptyException = true;
    }
    assertTrue(emptyException);
    if (Path.WINDOWS) {
      toStringTest("c:");
      toStringTest("c:/");
      toStringTest("c:foo");
      toStringTest("c:foo/bar");
      toStringTest("c:foo/bar");
      toStringTest("c:/foo/bar");
      toStringTest("C:/foo/bar#boo");
      toStringTest("C:foo/bar#boo");
    }
  }

  private void toStringTest(String pathString) {
    assertEquals(pathString, new Path(pathString).toString());
  }

  public void testNormalize() {
    assertEquals("/", new Path("//").toString());
    assertEquals("/foo", new Path("/foo/").toString());
    assertEquals("/foo", new Path("/foo/").toString());
    assertEquals("foo", new Path("foo/").toString());
    assertEquals("foo", new Path("foo//").toString());
    assertEquals("foo/bar", new Path("foo//bar").toString());
    if (Path.WINDOWS) {
      assertEquals("c:/a/b", new Path("c:\\a\\b").toString());
    }
  }

  public void testIsAbsolute() {
    assertTrue(new Path("/").isAbsolute());
    assertTrue(new Path("/foo").isAbsolute());
    assertFalse(new Path("foo").isAbsolute());
    assertFalse(new Path("foo/bar").isAbsolute());
    assertFalse(new Path(".").isAbsolute());
    if (Path.WINDOWS) {
      assertTrue(new Path("c:/a/b").isAbsolute());
      assertFalse(new Path("c:a/b").isAbsolute());
    }
  }

  public void testParent() {
    assertEquals(new Path("/foo"), new Path("/foo/bar").getParent());
    assertEquals(new Path("foo"), new Path("foo/bar").getParent());
    assertEquals(new Path("/"), new Path("/foo").getParent());
    if (Path.WINDOWS) {
      assertEquals(new Path("c:/"), new Path("c:/foo").getParent());
    }
  }

  public void testChild() {
    assertEquals(new Path("."), new Path(".", "."));
    assertEquals(new Path("/"), new Path("/", "."));
    assertEquals(new Path("/"), new Path(".", "/"));
    assertEquals(new Path("/foo"), new Path("/", "foo"));
    assertEquals(new Path("/foo/bar"), new Path("/foo", "bar"));
    assertEquals(new Path("/foo/bar/baz"), new Path("/foo/bar", "baz"));
    assertEquals(new Path("/foo/bar/baz"), new Path("/foo", "bar/baz"));
    assertEquals(new Path("foo"), new Path(".", "foo"));
    assertEquals(new Path("foo/bar"), new Path("foo", "bar"));
    assertEquals(new Path("foo/bar/baz"), new Path("foo", "bar/baz"));
    assertEquals(new Path("foo/bar/baz"), new Path("foo/bar", "baz"));
    assertEquals(new Path("/foo"), new Path("/bar", "/foo"));
    if (Path.WINDOWS) {
      assertEquals(new Path("c:/foo"), new Path("/bar", "c:/foo"));
      assertEquals(new Path("c:/foo"), new Path("d:/bar", "c:/foo"));
    }
  }
  
  public void testEquals() {
    assertFalse(new Path("/").equals(new Path("/foo")));
  }

  public void testDots() {
    // Test Path(String) 
    assertEquals(new Path("/foo/bar/baz").toString(), "/foo/bar/baz");
    assertEquals(new Path("/foo/bar", ".").toString(), "/foo/bar");
    assertEquals(new Path("/foo/bar/../baz").toString(), "/foo/baz");
    assertEquals(new Path("/foo/bar/./baz").toString(), "/foo/bar/baz");
    assertEquals(new Path("/foo/bar/baz/../../fud").toString(), "/foo/fud");
    assertEquals(new Path("/foo/bar/baz/.././../fud").toString(), "/foo/fud");
    assertEquals(new Path("../../foo/bar").toString(), "../../foo/bar");
    assertEquals(new Path(".././../foo/bar").toString(), "../../foo/bar");
    assertEquals(new Path("./foo/bar/baz").toString(), "foo/bar/baz");
    assertEquals(new Path("/foo/bar/../../baz/boo").toString(), "/baz/boo");
    assertEquals(new Path("foo/bar/").toString(), "foo/bar");
    assertEquals(new Path("foo/bar/../baz").toString(), "foo/baz");
    assertEquals(new Path("foo/bar/../../baz/boo").toString(), "baz/boo");
    
    
    // Test Path(Path,Path)
    assertEquals(new Path("/foo/bar", "baz/boo").toString(), "/foo/bar/baz/boo");
    assertEquals(new Path("foo/bar/","baz/bud").toString(), "foo/bar/baz/bud");
    
    assertEquals(new Path("/foo/bar","../../boo/bud").toString(), "/boo/bud");
    assertEquals(new Path("foo/bar","../../boo/bud").toString(), "boo/bud");
    assertEquals(new Path(".","boo/bud").toString(), "boo/bud");

    assertEquals(new Path("/foo/bar/baz","../../boo/bud").toString(), "/foo/boo/bud");
    assertEquals(new Path("foo/bar/baz","../../boo/bud").toString(), "foo/boo/bud");

    
    assertEquals(new Path("../../","../../boo/bud").toString(), "../../../../boo/bud");
    assertEquals(new Path("../../foo","../../../boo/bud").toString(), "../../../../boo/bud");
    assertEquals(new Path("../../foo/bar","../boo/bud").toString(), "../../foo/boo/bud");

    assertEquals(new Path("foo/bar/baz","../../..").toString(), "");
    assertEquals(new Path("foo/bar/baz","../../../../..").toString(), "../..");
  }
  
  public void testScheme() throws java.io.IOException {
    assertEquals("foo:/bar", new Path("foo:/","/bar").toString()); 
    assertEquals("foo://bar/baz", new Path("foo://bar/","/baz").toString()); 
  }

  public void testURI() throws URISyntaxException, IOException {
    URI uri = new URI("file:///bar#baz");
    Path path = new Path(uri);
    assertTrue(uri.equals(new URI(path.toString())));
    
    FileSystem fs = path.getFileSystem(new Configuration());
    assertTrue(uri.equals(new URI(fs.makeQualified(path).toString())));
    
    // uri without hash
    URI uri2 = new URI("file:///bar/baz");
    assertTrue(
      uri2.equals(new URI(fs.makeQualified(new Path(uri2)).toString())));
    assertEquals("foo://bar/baz#boo", new Path("foo://bar/", new Path(new URI(
        "/baz#boo"))).toString());
    assertEquals("foo://bar/baz/fud#boo", new Path(new Path(new URI(
        "foo://bar/baz#bud")), new Path(new URI("fud#boo"))).toString());
    // if the child uri is absolute path
    assertEquals("foo://bar/fud#boo", new Path(new Path(new URI(
        "foo://bar/baz#bud")), new Path(new URI("/fud#boo"))).toString());
 }
  
  public void testGlobEscapeStatus() throws Exception {
    FileSystem lfs = FileSystem.getLocal(new Configuration());
    Path testRoot = lfs.makeQualified(new Path(System.getProperty(
        "test.build.data", "test/build/data"), "testPathGlob"));
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);
    assertTrue(lfs.getFileStatus(testRoot).isDir());
    lfs.setWorkingDirectory(testRoot);

    // create a couple dirs with file in them
    Path paths[] = new Path[] { new Path(testRoot, "*/f"),
        new Path(testRoot, "d1/f"), new Path(testRoot, "d2/f") };
    Arrays.sort(paths);
    for (Path p : paths) {
      lfs.create(p).close();
      assertTrue(lfs.exists(p));
    }
    // try the non-globbed listStatus
    FileStatus stats[] = lfs.listStatus(new Path(testRoot, "*"));
    assertEquals(1, stats.length);
    assertEquals(new Path(testRoot, "*/f"), stats[0].getPath());

    // ensure globStatus with "*" finds all dir contents
    stats = lfs.globStatus(new Path(testRoot, "*"));
    Arrays.sort(stats);
    assertEquals(paths.length, stats.length);
    for (int i = 0; i < paths.length; i++) {
      assertEquals(paths[i].getParent(), stats[i].getPath());
    }

    // ensure that globStatus with an escaped "\*" only finds "*"
    stats = lfs.globStatus(new Path(testRoot, "\\*"));
    assertEquals(1, stats.length);
    assertEquals(new Path(testRoot, "*"), stats[0].getPath());

    // try to glob the inner file for all dirs
    stats = lfs.globStatus(new Path(testRoot, "*/f"));
    assertEquals(paths.length, stats.length);
    for (int i = 0; i < paths.length; i++) {
      assertEquals(paths[i], stats[i].getPath());
    }

    // try to get the inner file for only the "*" dir
    stats = lfs.globStatus(new Path(testRoot, "\\*/f"));
    assertEquals(1, stats.length);
    assertEquals(new Path(testRoot, "*/f"), stats[0].getPath());

    // try to glob all the contents of the "*" dir
    stats = lfs.globStatus(new Path(testRoot, "\\*/*"));
    assertEquals(1, stats.length);
    assertEquals(new Path(testRoot, "*/f"), stats[0].getPath());
  }

}
