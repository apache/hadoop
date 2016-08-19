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
package org.apache.hadoop.fs.s3a;

import com.google.common.base.Joiner;
import com.google.common.collect.Ordering;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSTestWrapper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestWrapper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.TestPath;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Globbing test. These tests include patterns relative to the user
 * home directory, so cannot be parallelized with any other test which
 * do that.
 */
public class TestS3AGlobPaths extends AbstractS3ATestBase {

  static class RegexPathFilter implements PathFilter {

    private final String regex;

    public RegexPathFilter(String regex) {
      this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
      return path.toString().matches(regex);
    }

  }

  private S3AFileSystem fs;
  private static final int NUM_OF_PATHS = 4;
  private Path userPath;
  private String userDir;
  private String userDirQuoted;

  private final Path[] path = new Path[NUM_OF_PATHS];

  @Override
  public void setup() throws Exception {
    super.setup();
    fs = getFileSystem();
    userPath = fs.getHomeDirectory();
    userDir = userPath.toUri().getPath().toString();
    userDirQuoted = Pattern.quote(userDir);
  }

  @After
  public void cleanupFS() throws IOException {
    if (fs != null) {
      fs.delete(userPath, true);
    }
  }

  @Test
  public void testMultiGlob() throws IOException {
    FileStatus[] status;
    /*
     *  /dir1/subdir1
     *  /dir1/subdir1/f1
     *  /dir1/subdir1/f2
     *  /dir1/subdir2/f1
     *  /dir2/subdir1
     *  /dir2/subdir2
     *  /dir2/subdir2/f1
     *  /dir3/f1
     *  /dir3/f1
     *  /dir3/f2(dir)
     *  /dir3/subdir2(file)
     *  /dir3/subdir3
     *  /dir3/subdir3/f1
     *  /dir3/subdir3/f1/f1
     *  /dir3/subdir3/f3
     *  /dir4
     */

    Path d1 = new Path(userDir, "dir1");
    Path d11 = new Path(d1, "subdir1");
    Path d12 = new Path(d1, "subdir2");

    Path f111 = new Path(d11, "f1");
    fs.createNewFile(f111);
    Path f112 = new Path(d11, "f2");
    fs.createNewFile(f112);
    Path f121 = new Path(d12, "f1");
    fs.createNewFile(f121);

    Path d2 = new Path(userDir, "dir2");
    Path d21 = new Path(d2, "subdir1");
    fs.mkdirs(d21);
    Path d22 = new Path(d2, "subdir2");
    Path f221 = new Path(d22, "f1");
    fs.createNewFile(f221);

    Path d3 = new Path(userDir, "dir3");
    Path f31 = new Path(d3, "f1");
    fs.createNewFile(f31);
    Path d32 = new Path(d3, "f2");
    fs.mkdirs(d32);
    Path f32 = new Path(d3, "subdir2"); // fake as a subdir!
    fs.createNewFile(f32);
    Path d33 = new Path(d3, "subdir3");
    Path f333 = new Path(d33, "f3");
    fs.createNewFile(f333);
    Path d331 = new Path(d33, "f1");
    Path f3311 = new Path(d331, "f1");
    fs.createNewFile(f3311);
    Path d4 = new Path(userDir, "dir4");
    fs.mkdirs(d4);

    /*
     * basic
     */
    Path root = userPath;
    checkStatus(fs.globStatus(root), root);

    status = fs.globStatus(new Path(userDir, "x"));
    assertNull(status);

    status = fs.globStatus(new Path("x"));
    assertNull(status);

    status = fs.globStatus(new Path(userDir, "x/x"));
    assertNull(status);

    status = fs.globStatus(new Path("x/x"));
    assertNull(status);

    status = fs.globStatus(new Path(userDir, "*"));
    checkStatus(status, d1, d2, d3, d4);

    status = fs.globStatus(new Path("*"));
    checkStatus(status, d1, d2, d3, d4);

    status = fs.globStatus(new Path(userDir, "*/x"));
    checkStatus(status);

    status = fs.globStatus(new Path("*/x"));
    checkStatus(status);

    status = fs.globStatus(new Path(userDir, "x/*"));
    checkStatus(status);

    status = fs.globStatus(new Path("x/*"));
    checkStatus(status);

    // make sure full pattern is scanned instead of bailing early with undef
    status = fs.globStatus(new Path(userDir, "x/x/x/*"));
    checkStatus(status);

    status = fs.globStatus(new Path("x/x/x/*"));
    checkStatus(status);

    status = fs.globStatus(new Path(userDir, "*/*"));
    checkStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);

    status = fs.globStatus(new Path("*/*"));
    checkStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);

    /*
     * one level deep
     */
    status = fs.globStatus(new Path(userDir, "dir*/*"));
    checkStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);

    status = fs.globStatus(new Path("dir*/*"));
    checkStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);

    status = fs.globStatus(new Path(userDir, "dir*/subdir*"));
    checkStatus(status, d11, d12, d21, d22, f32, d33);

    status = fs.globStatus(new Path("dir*/subdir*"));
    checkStatus(status, d11, d12, d21, d22, f32, d33);

    status = fs.globStatus(new Path(userDir, "dir*/f*"));
    checkStatus(status, f31, d32);

    status = fs.globStatus(new Path("dir*/f*"));
    checkStatus(status, f31, d32);

    /*
     * subdir1 globs
     */
    status = fs.globStatus(new Path(userPath, "dir*/subdir1"));
    checkStatus(status, d11, d21);

    status = fs.globStatus(new Path(userPath, "dir*/subdir1/*"));
    checkStatus(status, f111, f112);

    status = fs.globStatus(new Path(userPath, "dir*/subdir1/*/*"));
    checkStatus(status);

    status = fs.globStatus(new Path(userPath, "dir*/subdir1/x"));
    checkStatus(status);

    status = fs.globStatus(new Path(userPath, "dir*/subdir1/x*"));
    checkStatus(status);

    /*
     * subdir2 globs
     */
    status = fs.globStatus(new Path(userPath, "dir*/subdir2"));
    checkStatus(status, d12, d22, f32);

    status = fs.globStatus(new Path(userPath, "dir*/subdir2/*"));
    checkStatus(status, f121, f221);

    status = fs.globStatus(new Path(userPath, "dir*/subdir2/*/*"));
    checkStatus(status);

    /*
     * subdir3 globs
     */
    status = fs.globStatus(new Path(userPath, "dir*/subdir3"));
    checkStatus(status, d33);

    status = fs.globStatus(new Path(userPath, "dir*/subdir3/*"));
    checkStatus(status, d331, f333);

    status = fs.globStatus(new Path(userPath, "dir*/subdir3/*/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(userPath, "dir*/subdir3/*/*/*"));
    checkStatus(status);

    /*
     * file1 single dir globs
     */
    status = fs.globStatus(new Path(userPath, "dir*/subdir1/f1"));
    checkStatus(status, f111);

    status = fs.globStatus(new Path(userPath, "dir*/subdir1/f1*"));
    checkStatus(status, f111);

    status = fs.globStatus(new Path(userPath, "dir*/subdir1/f1/*"));
    checkStatus(status);

    status = fs.globStatus(new Path(userPath, "dir*/subdir1/f1*/*"));
    checkStatus(status);

    /*
     * file1 multi-dir globs
     */
    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f1"));
    checkStatus(status, f111, f121, f221, d331);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f1*"));
    checkStatus(status, f111, f121, f221, d331);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f1/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f1*/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f1*/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f1*/x"));
    checkStatus(status);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f1*/*/*"));
    checkStatus(status);

    /*
     *  file glob multiple files
     */

    status = fs.globStatus(new Path(userPath, "dir*/subdir*"));
    checkStatus(status, d11, d12, d21, d22, f32, d33);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/*"));
    checkStatus(status, f111, f112, f121, f221, d331, f333);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f*"));
    checkStatus(status, f111, f112, f121, f221, d331, f333);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/f*/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/*/f1"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(userPath, "dir*/subdir*/*/*"));
    checkStatus(status, f3311);

    // doesn't exist
    status = fs.globStatus(new Path(userPath, "dir*/subdir1/f3"));
    checkStatus(status);

    status = fs.globStatus(new Path(userPath, "dir*/subdir1/f3*"));
    checkStatus(status);

    status = fs.globStatus(new Path("{x}"));
    checkStatus(status);

    status = fs.globStatus(new Path("{x,y}"));
    checkStatus(status);

    status = fs.globStatus(new Path("dir*/{x,y}"));
    checkStatus(status);

    status = fs.globStatus(new Path("dir*/{f1,y}"));
    checkStatus(status, f31);

    status = fs.globStatus(new Path("{x,y}"));
    checkStatus(status);

    status = fs.globStatus(new Path("/{x/x,y/y}"));
    checkStatus(status);

    status = fs.globStatus(new Path("{x/x,y/y}"));
    checkStatus(status);

    status = fs.globStatus(new Path(Path.CUR_DIR));
    checkStatus(status, userPath);

    status = fs.globStatus(new Path(userDir + "{/dir1}"));
    checkStatus(status, d1);

    status = fs.globStatus(new Path(userDir + "{/dir*}"));
    checkStatus(status, d1, d2, d3, d4);

    status = fs.globStatus(new Path(Path.SEPARATOR), trueFilter);
    checkStatus(status, new Path(Path.SEPARATOR));

    status = fs.globStatus(new Path(Path.CUR_DIR), trueFilter);
    checkStatus(status, userPath);

    status = fs.globStatus(d1, trueFilter);
    checkStatus(status, d1);

    status = fs.globStatus(userPath, trueFilter);
    checkStatus(status, userPath);

    status = fs.globStatus(new Path(userPath, "*"), trueFilter);
    checkStatus(status, d1, d2, d3, d4);

    status = fs.globStatus(new Path("/x/*"), trueFilter);
    checkStatus(status);

    status = fs.globStatus(new Path("/x"), trueFilter);
    assertNull(status);

    status = fs.globStatus(new Path("/x/x"), trueFilter);
    assertNull(status);

    /*
     * false filter
     */
    PathFilter falseFilter = new PathFilter() {
      @Override
      public boolean accept(Path p) {
        return false;
      }
    };

    status = fs.globStatus(new Path(Path.SEPARATOR), falseFilter);
    assertNull(status);

    status = fs.globStatus(new Path(Path.CUR_DIR), falseFilter);
    assertNull(status);

    status = fs.globStatus(userPath, falseFilter);
    assertNull(status);

    status = fs.globStatus(new Path(userPath, "*"), falseFilter);
    checkStatus(status);

    status = fs.globStatus(new Path("/x/*"), falseFilter);
    checkStatus(status);

    status = fs.globStatus(new Path("/x"), falseFilter);
    assertNull(status);

    status = fs.globStatus(new Path("/x/x"), falseFilter);
    assertNull(status);

  }

  private void checkStatus(FileStatus[] status, Path... expectedMatches) {
    assertNotNull(status);
    String[] paths = new String[status.length];
    for (int i = 0; i < status.length; i++) {
      paths[i] = getPathFromStatus(status[i]);
    }
    // get the sort paths of the expected values
    String[] matches = new String[expectedMatches.length];
    for (int i = 0; i < expectedMatches.length; i++) {
      matches[i] = expectedMatches[i].toUri().getPath();
    }
    String got = StringUtils.join(paths, "\n");
    String expected = StringUtils.join(matches, "\n");
    assertEquals(expected, got);
  }

  private String getPathFromStatus(FileStatus status) {
//    return status.getPath().toUri().toString();
    return status.getPath().toUri().getPath();
  }

  @Test
  public void testPathFilter() throws IOException {
    String[] files = {
        userDir + "/a",
        userDir + "/a/b"
    };
    Path[] matchedPath = prepareTesting(
        userDir + "/*/*", files,
        new RegexPathFilter("^.*" + userDirQuoted + "/a/b"));
    assertEquals(1, matchedPath.length);
    assertEquals(path[1], matchedPath[0]);
  }

  @Test
  public void testPathFilterWithFixedLastComponent() throws IOException {
    String[] files = {
        userDir + "/a",
        userDir + "/a/b",
        userDir + "/c",
        userDir + "/c/b",
    };
    Path[] matchedPath = prepareTesting(userDir + "/*/b", files,
        new RegexPathFilter("^.*" + userDirQuoted + "/a/b"));
    assertEquals(1, matchedPath.length);
    assertEquals(path[1], matchedPath[0]);
  }

  @Test
  public void pTestLiteral() throws IOException {
    String[] files = {
        userDir + "/a2c",
        userDir + "/abc.d"
    };
    assertMatchOperation(userDir + "/abc.d", files, 1);
  }

  @Test
  public void pTestEscape() throws IOException {
    String[] files = {
        userDir + "/ab\\[c.d"
    };
    assertMatchOperation(userDir + "/ab\\[c.d", files, 0);
  }

  @Test
  public void pTestAny() throws IOException {
    String[] files = {
        userDir + "/abc",
        userDir + "/a2c",
        userDir + "/a.c",
        userDir + "/abcd"
    };
    assertMatchOperation(userDir + "/a?c", files, 2, 1, 0);
  }

  @Test
  public void pTestClosure1() throws IOException {
    String[] files = {
        userDir + "/a",
        userDir + "/abc",
        userDir + "/abc.p",
        userDir + "/bacd"
    };
    assertMatchOperation(userDir + "/a*", files, 0, 1, 2);
  }

  @Test
  public void pTestClosure2() throws IOException {
    String[] files = {
        userDir + "/a.",
        userDir + "/a.txt",
        userDir + "/a.old.java",
        userDir + "/.java"
    };
    assertMatchOperation(userDir + "/a.*", files, 0, 2, 1);
  }

  @Test
  public void pTestClosure3() throws IOException {
    String[] files = {
        userDir + "/a.txt.x",
        userDir + "/ax",
        userDir + "/ab37x",
        userDir + "/bacd"
    };
    assertMatchOperation(userDir + "/a*x", files, 0, 2, 1);
  }

  @Test
  public void pTestClosure4() throws IOException {
    String[] files = {
        userDir + "/dir1/file1",
        userDir + "/dir2/file2",
        userDir + "/dir3/file1"
    };
    assertMatchOperation(userDir + "/*/file1", files, 0, 2);
  }

  @Test
  public void pTestClosure5() throws IOException {
    String[] files = {
        userDir + "/dir1/file1",
        userDir + "/file1"
    };
    assertMatchOperation(userDir + "/*/file1", files, 0);
  }

  @Test
  public void pTestSet() throws IOException {
    String[] files = {
        userDir + "/a.c",
        userDir + "/a.cpp",
        userDir + "/a.hlp",
        userDir + "/a.hxy"
    };
    assertMatchOperation(userDir + "/a.[ch]??", files, 1, 2, 3);
  }

  @Test
  public void pTestRange() throws IOException {
    String[] files = {
        userDir + "/a.d",
        userDir + "/a.e",
        userDir + "/a.f",
        userDir + "/a.h"
    };
    assertMatchOperation(userDir + "/a.[d-fm]", files,
        0, 1, 2);
  }

  @Test
  public void pTestSetExcl() throws IOException {
    String[] files = {
        userDir + "/a.d",
        userDir + "/a.e",
        userDir + "/a.0",
        userDir + "/a.h"
    };
    assertMatchOperation(userDir + "/a.[^a-cg-z0-9]", files, 0, 1);
  }

  @Test
  public void pTestCombination() throws IOException {
    String[] files = {
        "/user/aa/a.c",
        "/user/bb/a.cpp",
        "/user1/cc/b.hlp",
        "/user/dd/a.hxy"
    };
    assertMatchOperation("/use?/*/a.[ch]{lp,xy}", files, 3);
  }

  /* Test {xx,yy} */
  @Test
  public void pTestCurlyBracket() throws IOException {
    String[] files = {
        userDir + "/a.abcxx",
        userDir + "/a.abxy",
        userDir + "/a.hlp",
        userDir + "/a.jhyy"
    };
    assertMatchOperation(userDir + "/a.{abc,jh}??", files, 0, 3);
  }

  @Test
  public void testNestedCurlyBracket() throws Throwable {
    String[] files = {
        userDir + "/a.abcxx",
        userDir + "/a.abdxy",
        userDir + "/a.hlp",
        userDir + "/a.jhyy"
    };
    assertMatchOperation(userDir + "/a.{ab{c,d},jh}??", files, 0, 1, 3);
  }

  @Test
  public void testCrossComponentCurlyBrackets() throws Throwable {
    // cross-component curlies
    String[] files = {
        userDir + "/a/b",
        userDir + "/a/d",
        userDir + "/c/b",
        userDir + "/c/d"
    };
    assertMatchOperation(userDir + "/{a/b,c/d}", files, 0, 3);
  }

  public Path[] assertMatchOperation(String pattern,
      String[] files,
      int... matchIndices)
      throws IOException {
    Path[] matchedPaths = prepareTesting(pattern, files);
    int expectedLength = matchIndices.length;
    StringBuilder builder = new StringBuilder(
        expectedLength * 128);
    builder.append("Expected Paths\n");
    for (int index : matchIndices) {
      if (index < path.length) {
        builder.append(
            String.format("  [%d] %s\n", index, path[index]));
      }
    }
    Joiner j = Joiner.on("\n  ");
    builder.append("\nMatched paths:\n  ");
    j.appendTo(builder, matchedPaths);
    assertEquals(builder.toString(), expectedLength, matchedPaths.length);
    for (int i = 0; i < matchedPaths.length; i++) {
      int expectedIndex = matchIndices[i];
      Path expectedPath = path[expectedIndex];
      assertEquals(String.format("Element %d: in %s", i, builder.toString()),
          expectedPath, matchedPaths[i]);
    }
    return matchedPaths;
  }

  // cross-component absolute curlies
  @Test
  public void testCrossComponentAbsoluteCurlyBrackets() throws Throwable {
    // cross-component curlies
    String[] files = {
        "/a/b",
        "/a/d",
        "/c/b",
        "/c/d"
    };
    assertMatchOperation("{/a/b,/c/d}", files, 0, 3);
  }

  @Test
  public void testStandalone() throws Throwable {
    String[] files = {
        userDir + "/}bc",
        userDir + "/}c"
    };
    assertMatchOperation(userDir + "/}{a,b}c", files, 0);
    // test {b}
    assertMatchOperation(userDir + "/}{b}c", files, 0);
    // test {}
    assertMatchOperation(userDir + "/}{}bc", files, 0);

    // test {,}
    assertMatchOperation(userDir + "/}{,}bc", files, 0);

    // test {b,}
    assertMatchOperation(userDir + "/}{b,}c", files, 0, 1);

    // test {,b}
    assertMatchOperation(userDir + "/}{,b}c", files, 0, 1);

    // test a combination of {} and ?
    assertMatchOperation(userDir + "/}{ac,?}", files, 1);

    // test ill-formed curly
    try {
      prepareTesting(userDir + "}{bc", files);
      fail("Expected exception");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Illegal file pattern:", e);
    }
  }

  /* test that a path name can contain Java regex special characters */
  @Test
  public void pTestJavaRegexSpecialChars() throws IOException {
    String[] files = {userDir + "/($.|+)bc", userDir + "/abc"};
    assertMatchOperation(userDir + "/($.|+)*", files, 0);

  }

  private Path[] prepareTesting(String pattern, String[] files)
      throws IOException {
    buildPaths(files);
    Path patternPath = new Path(pattern);
    Path[] globResults = FileUtil.stat2Paths(fs.globStatus(patternPath),
        patternPath);
    for (int i = 0; i < globResults.length; i++) {
      globResults[i] =
          globResults[i].makeQualified(fs.getUri(), fs.getWorkingDirectory());
    }
    return globResults;
  }

  private void buildPaths(String[] files) throws IOException {
    for (int i = 0; i < Math.min(NUM_OF_PATHS, files.length); i++) {
      path[i] = fs.makeQualified(new Path(files[i]));
      if (!fs.mkdirs(path[i])) {
        throw new IOException("Mkdirs failed to create " + path[i].toString());
      }
    }
  }

  private Path[] prepareTesting(String pattern, String[] files,
      PathFilter filter) throws IOException {
    buildPaths(files);
    Path patternPath = new Path(pattern);
    Path[] globResults = FileUtil.stat2Paths(fs.globStatus(patternPath, filter),
        patternPath);
    for (int i = 0; i < globResults.length; i++) {
      globResults[i] =
          globResults[i].makeQualified(fs.getUri(), fs.getWorkingDirectory());
    }
    return globResults;
  }

  /**
   * A glob test that can be run on either FileContext or FileSystem.
   */
  private abstract class FSTestWrapperGlobTest {
    FSTestWrapperGlobTest(boolean useFc) {
      this.wrap = new FileSystemTestWrapper(fs);
    }

    abstract void run() throws Exception;

    final FSTestWrapper wrap;
  }

  /**
   * Run a glob test on FileContext.
   */
  private void testOnFileSystem(FSTestWrapperGlobTest test) throws Exception {
    fs.mkdirs(userPath);
    test.run();
  }

  /**
   * Accept all paths.
   */
  private static class AcceptAllPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return true;
    }
  }

  private static final PathFilter trueFilter = new AcceptAllPathFilter();

  /**
   * Accept only paths ending in Z.
   */
  private static class AcceptPathsEndingInZ implements PathFilter {
    @Override
    public boolean accept(Path path) {
      String stringPath = path.toUri().getPath();
      return stringPath.endsWith("z");
    }
  }

  /**
   * Test that globStatus fills in the scheme even when it is not provided.
   */
  private class TestGlobFillsInScheme extends FSTestWrapperGlobTest {
    TestGlobFillsInScheme(boolean useFc) {
      super(useFc);
    }

    void run() throws Exception {
      // Verify that the default scheme is hdfs, when we don't supply one.
      wrap.mkdir(new Path(userPath, "/alpha"), FsPermission.getDirDefault(),
          false);
      wrap.createSymlink(new Path(userPath, "/alpha"), new Path(userPath
          + "/alphaLink"), false);
      FileStatus statuses[] = wrap.globStatus(
          new Path(userPath, "/alphaLink"), new AcceptAllPathFilter());
      Assert.assertEquals(1, statuses.length);
      Path p = statuses[0].getPath();
      Assert.assertEquals(userDir + "/alpha", p.toUri().getPath());
      Assert.assertEquals("hdfs", p.toUri().getScheme());

    }
  }

  @Test
  @Ignore
  public void testGlobFillsInSchemeOnFS() throws Exception {
    testOnFileSystem(new TestGlobFillsInScheme(false));
  }

  /**
   * Test that globStatus works with relative paths.
   **/
  private class TestRelativePath extends FSTestWrapperGlobTest {
    TestRelativePath(boolean useFc) {
      super(useFc);
    }

    void run() throws Exception {
      String[] files = {"a", "abc", "abc.p", "bacd"};

      Path[] p = new Path[files.length];
      for (int i = 0; i < files.length; i++) {
        p[i] = wrap.makeQualified(new Path(files[i]));
        wrap.mkdir(p[i], FsPermission.getDirDefault(), true);
      }

      Path patternPath = new Path("a*");
      Path[] globResults = FileUtil.stat2Paths(wrap.globStatus(patternPath,
          new AcceptAllPathFilter()),
          patternPath);

      for (int i = 0; i < globResults.length; i++) {
        globResults[i] = wrap.makeQualified(globResults[i]);
      }

      assertEquals(3, globResults.length);

      // The default working directory for FileSystem is the user's home
      // directory.  For FileContext, the default is based on the UNIX user that
      // started the jvm.  This is arguably a bug (see HADOOP-10944 for
      // details).  We work around it here by explicitly calling
      // getWorkingDirectory and going from there.
      String pwd = wrap.getWorkingDirectory().toUri().getPath();
      assertEquals(pwd + "/a;" + pwd + "/abc;" + pwd + "/abc.p",
          TestPath.mergeStatuses(globResults));
    }
  }

  @Test
  public void testRelativePathOnFS() throws Exception {
    testOnFileSystem(new TestRelativePath(false));
  }

  /**
   * Test trying to glob the root.  Regression test for HDFS-5888.
   **/
  private class TestGlobRoot extends FSTestWrapperGlobTest {
    TestGlobRoot(boolean useFc) {
      super(useFc);
    }

    void run() throws Exception {
      final Path rootPath = new Path("/");
      FileStatus oldRootStatus = wrap.getFileStatus(rootPath);
      FileStatus[] status =
          wrap.globStatus(rootPath, new AcceptAllPathFilter());
      Assert.assertEquals(1, status.length);
      // TODO: Add any way to check that this is a real status entry, not a fake one.
    }
  }

  @Test
  public void testGlobRootOnFS() throws Exception {
    testOnFileSystem(new TestGlobRoot(false));
  }

  /**
   * Test glob expressions that don't appear at the end of the path.  Regression
   * test for HADOOP-10957.
   **/
  private class TestNonTerminalGlobs extends FSTestWrapperGlobTest {
    TestNonTerminalGlobs(boolean useFc) {
      super(useFc);
    }

    void run() throws Exception {
      try {
        fs.mkdirs(new Path("/filed_away/alpha"));
        wrap.createFile(new Path("/filed"), 0);
        FileStatus[] statuses =
            wrap.globStatus(new Path("/filed*/alpha"),
                new AcceptAllPathFilter());
        Assert.assertEquals(1, statuses.length);
        Assert.assertEquals("/filed_away/alpha", statuses[0].getPath()
            .toUri().getPath());
        wrap.mkdir(new Path("/filed_away/alphabet"),
            new FsPermission((short) 0777), true);
        wrap.mkdir(new Path("/filed_away/alphabet/abc"),
            new FsPermission((short) 0777), true);
        statuses = wrap.globStatus(new Path("/filed*/alph*/*b*"),
            new AcceptAllPathFilter());
        Assert.assertEquals(1, statuses.length);
        Assert.assertEquals("/filed_away/alphabet/abc", statuses[0].getPath()
            .toUri().getPath());
      } finally {
        fs.delete(new Path("/filed"), true);
        fs.delete(new Path("/filed_away"), true);
      }
    }
  }

  @Test
  public void testNonTerminalGlobsOnFS() throws Exception {
    testOnFileSystem(new TestNonTerminalGlobs(false));
  }

  @Test
  public void testLocalFilesystem() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    String localTmp = System.getProperty("java.io.tmpdir");
    Path base = new Path(new Path(localTmp), UUID.randomUUID().toString());
    Assert.assertTrue(fs.mkdirs(base));
    Assert.assertTrue(fs.mkdirs(new Path(base, "e")));
    Assert.assertTrue(fs.mkdirs(new Path(base, "c")));
    Assert.assertTrue(fs.mkdirs(new Path(base, "a")));
    Assert.assertTrue(fs.mkdirs(new Path(base, "d")));
    Assert.assertTrue(fs.mkdirs(new Path(base, "b")));
    fs.deleteOnExit(base);
    FileStatus[] status = fs.globStatus(new Path(base, "*"));
    ArrayList<String> list = new ArrayList<>();
    for (FileStatus f : status) {
      list.add(f.getPath().toString());
    }
    boolean sorted = Ordering.natural().isOrdered(list);
    Assert.assertTrue(sorted);
  }
}

