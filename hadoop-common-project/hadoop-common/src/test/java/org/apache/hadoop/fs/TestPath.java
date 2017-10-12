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
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.AvroTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;

import com.google.common.base.Joiner;

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.apache.hadoop.test.PlatformAssumptions.assumeWindows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Hadoop Filesystem Paths.
 */
public class TestPath {
  /**
   * Merge a bunch of Path objects into a sorted semicolon-separated
   * path string.
   */
  public static String mergeStatuses(Path paths[]) {
    String pathStrings[] = new String[paths.length];
    int i = 0;
    for (Path path : paths) {
      pathStrings[i++] = path.toUri().getPath();
    }
    Arrays.sort(pathStrings);
    return Joiner.on(";").join(pathStrings);
  }

  /**
   * Merge a bunch of FileStatus objects into a sorted semicolon-separated
   * path string.
   */
  public static String mergeStatuses(FileStatus statuses[]) {
    Path paths[] = new Path[statuses.length];
    int i = 0;
    for (FileStatus status : statuses) {
      paths[i++] = status.getPath();
    }
    return mergeStatuses(paths);
  }

  @Test (timeout = 30000)
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

  @Test (timeout = 30000)
  public void testNormalize() throws URISyntaxException {
    assertEquals("", new Path(".").toString());
    assertEquals("..", new Path("..").toString());
    assertEquals("/", new Path("/").toString());
    assertEquals("/", new Path("//").toString());
    assertEquals("/", new Path("///").toString());
    assertEquals("//foo/", new Path("//foo/").toString());
    assertEquals("//foo/", new Path("//foo//").toString());
    assertEquals("//foo/bar", new Path("//foo//bar").toString());
    assertEquals("/foo", new Path("/foo/").toString());
    assertEquals("/foo", new Path("/foo/").toString());
    assertEquals("foo", new Path("foo/").toString());
    assertEquals("foo", new Path("foo//").toString());
    assertEquals("foo/bar", new Path("foo//bar").toString());
    assertEquals("hdfs://foo/foo2/bar/baz/",
        new Path(new URI("hdfs://foo//foo2///bar/baz///")).toString());
    if (Path.WINDOWS) {
      assertEquals("c:/a/b", new Path("c:\\a\\b").toString());
    }
  }

  @Test (timeout = 30000)
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

  @Test (timeout = 30000)
  public void testParent() {
    assertEquals(new Path("/foo"), new Path("/foo/bar").getParent());
    assertEquals(new Path("foo"), new Path("foo/bar").getParent());
    assertEquals(new Path("/"), new Path("/foo").getParent());
    assertEquals(null, new Path("/").getParent());
    if (Path.WINDOWS) {
      assertEquals(new Path("c:/"), new Path("c:/foo").getParent());
    }
  }

  @Test (timeout = 30000)
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

  @Test (timeout = 30000)
  public void testPathThreeArgContructor() {
    assertEquals(new Path("foo"), new Path(null, null, "foo"));
    assertEquals(new Path("scheme:///foo"), new Path("scheme", null, "/foo"));
    assertEquals(
        new Path("scheme://authority/foo"),
        new Path("scheme", "authority", "/foo"));

    if (Path.WINDOWS) {
      assertEquals(new Path("c:/foo/bar"), new Path(null, null, "c:/foo/bar"));
      assertEquals(new Path("c:/foo/bar"), new Path(null, null, "/c:/foo/bar"));
    } else {
      assertEquals(new Path("./a:b"), new Path(null, null, "a:b"));
    }

    // Resolution tests
    if (Path.WINDOWS) {
      assertEquals(
          new Path("c:/foo/bar"),
          new Path("/fou", new Path(null, null, "c:/foo/bar")));
      assertEquals(
          new Path("c:/foo/bar"),
          new Path("/fou", new Path(null, null, "/c:/foo/bar")));
      assertEquals(
          new Path("/foo/bar"),
          new Path("/foo", new Path(null, null, "bar")));
    } else {
      assertEquals(
          new Path("/foo/bar/a:b"),
          new Path("/foo/bar", new Path(null, null, "a:b")));
      assertEquals(
          new Path("/a:b"),
          new Path("/foo/bar", new Path(null, null, "/a:b")));
    }
  }

  @Test (timeout = 30000)
  public void testEquals() {
    assertFalse(new Path("/").equals(new Path("/foo")));
  }

  @Test (timeout = 30000)
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

  /** Test that Windows paths are correctly handled */
  @Test (timeout = 5000)
  public void testWindowsPaths() throws URISyntaxException, IOException {
    assumeWindows();

    assertEquals(new Path("c:\\foo\\bar").toString(), "c:/foo/bar");
    assertEquals(new Path("c:/foo/bar").toString(), "c:/foo/bar");
    assertEquals(new Path("/c:/foo/bar").toString(), "c:/foo/bar");
    assertEquals(new Path("file://c:/foo/bar").toString(), "file://c:/foo/bar");
  }

  /** Test invalid paths on Windows are correctly rejected */
  @Test (timeout = 5000)
  public void testInvalidWindowsPaths() throws URISyntaxException, IOException {
    assumeWindows();

    String [] invalidPaths = {
        "hdfs:\\\\\\tmp"
    };

    for (String path : invalidPaths) {
      try {
        Path item = new Path(path);
        fail("Did not throw for invalid path " + path);
      } catch (IllegalArgumentException iae) {
      }
    }
  }

  /** Test Path objects created from other Path objects */
  @Test (timeout = 30000)
  public void testChildParentResolution() throws URISyntaxException, IOException {
    Path parent = new Path("foo1://bar1/baz1");
    Path child  = new Path("foo2://bar2/baz2");
    assertEquals(child, new Path(parent, child));
  }

  @Test (timeout = 30000)
  public void testScheme() throws java.io.IOException {
    assertEquals("foo:/bar", new Path("foo:/","/bar").toString());
    assertEquals("foo://bar/baz", new Path("foo://bar/","/baz").toString());
  }

  @Test (timeout = 30000)
  public void testColonInPath() throws Exception {

    String path1Str = "x://dir:name/file";
    String path2Str = "x://dir/dir:2/file";
    String path3Str = "x://dir:name/dir:2/file";
    String path4Str = "x://dir/d3/log:file.gz";
    String path5Str = "file:///";
    String path6Str = "x:/";
    String path7Str = "/abc/bcd/dir:name/dir1:name1";
    String path8Str = "urn:example:animal:ferret:nose";
    String path9Str = ":/abc:/:";


    Path path1 = new Path(path1Str);
    Path path2 = new Path(path2Str);
    Path path3 = new Path(path3Str);
    Path path4 = new Path(path4Str);
    Path path5 = new Path(path5Str);
    Path path6 = new Path(path6Str);
    Path path7 = new Path(path7Str);
    Path path8 = new Path("./" +path8Str);
    Path path9 = new Path(path9Str);

    // String -> Path -> String
    assertEquals(path1Str, new Path(path1Str).toString());
    assertEquals(path2Str, new Path(path2Str).toString());
    assertEquals(path3Str, new Path(path3Str).toString());
    assertEquals(path4Str, new Path(path4Str).toString());
    assertEquals(path5Str, new Path(path5Str).toUri().toString());
    assertEquals(path6Str, new Path(path6Str).toString());
    assertEquals(path7Str, new Path(path7Str).toString());
    assertEquals("./"+path8Str, new Path(path8Str).toString());
    assertEquals("./"+path9Str, new Path(path9Str).toString());

    // String -> Path -> String -> Path
    assertEquals(path1, new Path(new Path(path1Str).toString()));
    assertEquals(path2, new Path(new Path(path2Str).toString()));
    assertEquals(path3, new Path(new Path(path3Str).toString()));
    assertEquals(path4, new Path(new Path(path4Str).toString()));
    assertEquals(path5, new Path(new Path(path5Str).toUri().toString()));
    assertEquals(path6, new Path(new Path(path6Str).toString()));
    assertEquals(path7, new Path(new Path(path7Str).toString()));
    assertEquals(path8, new Path(new Path(path8Str).toString()));

    // Path -> URI -> Path
    assertEquals(path1, new Path(path1.toUri()));
    assertEquals(path2, new Path(path2.toUri()));
    assertEquals(path3, new Path(path3.toUri()));
    assertEquals(path4, new Path(path4.toUri()));
    assertEquals(path5, new Path(path5.toUri()));
    assertEquals(path6, new Path(path6.toUri()));
    assertEquals(path7, new Path(path7.toUri()));
    assertEquals(path8, new Path(path8.toUri()));

    // Path -> URI -> String -> Path
    assertEquals(path1, new Path(new Path(path1Str).toUri().toString()));
    assertEquals(path2, new Path(new Path(path2Str).toUri().toString()));
    assertEquals(path3, new Path(new Path(path3Str).toUri().toString()));
    assertEquals(path4, new Path(new Path(path4Str).toUri().toString()));
    assertEquals(path5, new Path(new Path(path5Str).toUri().toString()));
    assertEquals(path6, new Path(new Path(path6Str).toUri().toString()));
    assertEquals(path7, new Path(new Path(path7Str).toUri().toString()));
    assertEquals(path8, new Path(new Path(path8Str).toUri().toString()));


    String path1Part1 = "x://dir:name";
    String path1Part2 = "/file";
    String path2Part1 = "x://dir/dir:2";
    String path2Part2 = "file";
    String path3Part1 = "x://dir:name";
    String path3Part2 = "/dir:2/file";
    String path4Part1 = "x://dir/d3";
    String path4Part2 = "log:file.gz";
    String path7Part1 = "/abc/bcd/dir:name/";
    String path7Part2 = "dir1:name1";
    String path8Part1 = "x://abc/:";
    String path8Part2 = "/:/:/:/:.bcd";

    assertEquals(path1Str, new Path(path1Part1, path1Part2).toString());
    assertEquals(path2Str, new Path(path2Part1, path2Part2).toString());
    assertEquals(path3Str, new Path(path3Part1, path3Part2).toString());
    assertEquals(path4Str, new Path(path4Part1, path4Part2).toString());
    assertEquals(path7Str, new Path(path7Part1, path7Part2).toString());
    assertEquals("x://abc/:/:/:/:.bcd", new Path(path8Part1, path8Part2).toString());
  }

  @Test (timeout = 30000)
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

  /** Test URIs created from Path objects */
  @Test (timeout = 30000)
  public void testPathToUriConversion() throws URISyntaxException, IOException {
    // Path differs from URI in that it ignores the query part..
    assertEquals("? mark char in to URI",
            new URI(null, null, "/foo?bar", null, null),
            new Path("/foo?bar").toUri());
    assertEquals("escape slashes chars in to URI",
            new URI(null, null, "/foo\"bar", null, null),
            new Path("/foo\"bar").toUri());
    assertEquals("spaces in chars to URI",
            new URI(null, null, "/foo bar", null, null),
            new Path("/foo bar").toUri());
    // therefore "foo?bar" is a valid Path, so a URI created from a Path
    // has path "foo?bar" where in a straight URI the path part is just "foo"
    assertEquals("/foo?bar",
            new Path("http://localhost/foo?bar").toUri().getPath());
    assertEquals("/foo",     new URI("http://localhost/foo?bar").getPath());

    // The path part handling in Path is equivalent to URI
    assertEquals(new URI("/foo;bar").getPath(), new Path("/foo;bar").toUri().getPath());
    assertEquals(new URI("/foo;bar"), new Path("/foo;bar").toUri());
    assertEquals(new URI("/foo+bar"), new Path("/foo+bar").toUri());
    assertEquals(new URI("/foo-bar"), new Path("/foo-bar").toUri());
    assertEquals(new URI("/foo=bar"), new Path("/foo=bar").toUri());
    assertEquals(new URI("/foo,bar"), new Path("/foo,bar").toUri());
  }

  /** Test reserved characters in URIs (and therefore Paths) */
  @Test (timeout = 30000)
  public void testReservedCharacters() throws URISyntaxException, IOException {
    // URI encodes the path
    assertEquals("/foo%20bar",
            new URI(null, null, "/foo bar", null, null).getRawPath());
    // URI#getPath decodes the path
    assertEquals("/foo bar",
            new URI(null, null, "/foo bar", null, null).getPath());
    // URI#toString returns an encoded path
    assertEquals("/foo%20bar",
            new URI(null, null, "/foo bar", null, null).toString());
    assertEquals("/foo%20bar", new Path("/foo bar").toUri().toString());
    // Reserved chars are not encoded
    assertEquals("/foo;bar",   new URI("/foo;bar").getPath());
    assertEquals("/foo;bar",   new URI("/foo;bar").getRawPath());
    assertEquals("/foo+bar",   new URI("/foo+bar").getPath());
    assertEquals("/foo+bar",   new URI("/foo+bar").getRawPath());

    // URI#getPath decodes the path part (and URL#getPath does not decode)
    assertEquals("/foo bar",
            new Path("http://localhost/foo bar").toUri().getPath());
    assertEquals("/foo%20bar",
            new Path("http://localhost/foo bar").toUri().toURL().getPath());
    assertEquals("/foo?bar",
            new URI("http", "localhost", "/foo?bar", null, null).getPath());
    assertEquals("/foo%3Fbar",
            new URI("http", "localhost", "/foo?bar", null, null).
                toURL().getPath());
  }

  @Test (timeout = 30000)
  public void testMakeQualified() throws URISyntaxException {
    URI defaultUri = new URI("hdfs://host1/dir1");
    URI wd         = new URI("hdfs://host2/dir2");

    // The scheme from defaultUri is used but the path part is not
    assertEquals(new Path("hdfs://host1/dir/file"),
        new Path("file").makeQualified(defaultUri, new Path("/dir")));

    // The defaultUri is only used if the path + wd has no scheme    
    assertEquals(new Path("hdfs://host2/dir2/file"),
                 new Path("file").makeQualified(defaultUri, new Path(wd)));
 }

  @Test (timeout = 30000)
  public void testGetName() {
    assertEquals("", new Path("/").getName());
    assertEquals("foo", new Path("foo").getName());
    assertEquals("foo", new Path("/foo").getName());
    assertEquals("foo", new Path("/foo/").getName());
    assertEquals("bar", new Path("/foo/bar").getName());
    assertEquals("bar", new Path("hdfs://host/foo/bar").getName());
  }
  
  @Test (timeout = 30000)
  public void testAvroReflect() throws Exception {
    AvroTestUtil.testReflect
      (new Path("foo"),
       "{\"type\":\"string\",\"java-class\":\"org.apache.hadoop.fs.Path\"}");
  }

  @Test (timeout = 30000)
  public void testGlobEscapeStatus() throws Exception {
    // This test is not meaningful on Windows where * is disallowed in file name.
    assumeNotWindows();
    FileSystem lfs = FileSystem.getLocal(new Configuration());
    Path testRoot = lfs.makeQualified(
        new Path(GenericTestUtils.getTempPath("testPathGlob")));
    lfs.delete(testRoot, true);
    lfs.mkdirs(testRoot);
    assertTrue(lfs.isDirectory(testRoot));
    lfs.setWorkingDirectory(testRoot);
    
    // create a couple dirs with file in them
    Path paths[] = new Path[]{
        new Path(testRoot, "*/f"),
        new Path(testRoot, "d1/f"),
        new Path(testRoot, "d2/f")
    };
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
    Path parentPaths[] = new Path[paths.length];
    for (int i = 0; i < paths.length; i++) {
      parentPaths[i] = paths[i].getParent();
    }
    assertEquals(mergeStatuses(parentPaths), mergeStatuses(stats));

    // ensure that globStatus with an escaped "\*" only finds "*"
    stats = lfs.globStatus(new Path(testRoot, "\\*"));
    assertEquals(1, stats.length);
    assertEquals(new Path(testRoot, "*"), stats[0].getPath());

    // try to glob the inner file for all dirs
    stats = lfs.globStatus(new Path(testRoot, "*/f"));
    assertEquals(paths.length, stats.length);
    assertEquals(mergeStatuses(paths), mergeStatuses(stats));

    // try to get the inner file for only the "*" dir
    stats = lfs.globStatus(new Path(testRoot, "\\*/f"));
    assertEquals(1, stats.length);
    assertEquals(new Path(testRoot, "*/f"), stats[0].getPath());

    // try to glob all the contents of the "*" dir
    stats = lfs.globStatus(new Path(testRoot, "\\*/*"));
    assertEquals(1, stats.length);
    assertEquals(new Path(testRoot, "*/f"), stats[0].getPath());
  }

  @Test (timeout = 30000)
  public void testMergePaths() {
    assertEquals(new Path("/foo/bar"),
      Path.mergePaths(new Path("/foo"),
        new Path("/bar")));

    assertEquals(new Path("/foo/bar/baz"),
      Path.mergePaths(new Path("/foo/bar"),
        new Path("/baz")));

    assertEquals(new Path("/foo/bar/baz"),
      Path.mergePaths(new Path("/foo"),
        new Path("/bar/baz")));

    assertEquals(new Path(Shell.WINDOWS ? "/C:/foo/bar" : "/C:/foo/C:/bar"),
      Path.mergePaths(new Path("/C:/foo"),
        new Path("/C:/bar")));

    assertEquals(new Path(Shell.WINDOWS ? "/C:/bar" : "/C:/C:/bar"),
        Path.mergePaths(new Path("/C:/"),
          new Path("/C:/bar")));

    assertEquals(new Path("/bar"),
        Path.mergePaths(new Path("/"), new Path("/bar")));

    assertEquals(new Path("viewfs:///foo/bar"),
      Path.mergePaths(new Path("viewfs:///foo"),
        new Path("file:///bar")));

    assertEquals(new Path("viewfs://vfsauthority/foo/bar"),
      Path.mergePaths(new Path("viewfs://vfsauthority/foo"),
        new Path("file://fileauthority/bar")));
  }

  @Test (timeout = 30000)
  public void testIsWindowsAbsolutePath() {
    assumeWindows();
    assertTrue(Path.isWindowsAbsolutePath("C:\\test", false));
    assertTrue(Path.isWindowsAbsolutePath("C:/test", false));
    assertTrue(Path.isWindowsAbsolutePath("/C:/test", true));
    assertFalse(Path.isWindowsAbsolutePath("/test", false));
    assertFalse(Path.isWindowsAbsolutePath("/test", true));
    assertFalse(Path.isWindowsAbsolutePath("C:test", false));
    assertFalse(Path.isWindowsAbsolutePath("/C:test", true));
  }

  @Test(timeout = 30000)
  public void testSerDeser() throws Throwable {
    Path source = new Path("hdfs://localhost:4040/scratch");
    ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
    try(ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(source);
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      Path deser = (Path) ois.readObject();
      Assert.assertEquals(source, deser);
    }

  }
}
