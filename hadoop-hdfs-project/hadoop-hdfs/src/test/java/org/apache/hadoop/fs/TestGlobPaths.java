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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

public class TestGlobPaths {
  
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
  
  static private MiniDFSCluster dfsCluster;
  static private FileSystem fs;
  static final private int NUM_OF_PATHS = 4;
  static private String USER_DIR;
  private Path[] path = new Path[NUM_OF_PATHS];
  
  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    fs = FileSystem.get(conf);
    USER_DIR = fs.getHomeDirectory().toUri().getPath().toString();
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    if(dfsCluster!=null) {
      dfsCluster.shutdown();
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

    Path d1 = new Path(USER_DIR, "dir1");
    Path d11 = new Path(d1, "subdir1");
    Path d12 = new Path(d1, "subdir2");
    
    Path f111 = new Path(d11, "f1");
    fs.createNewFile(f111);
    Path f112 = new Path(d11, "f2");
    fs.createNewFile(f112);
    Path f121 = new Path(d12, "f1");
    fs.createNewFile(f121);
    
    Path d2 = new Path(USER_DIR, "dir2");
    Path d21 = new Path(d2, "subdir1");
    fs.mkdirs(d21);
    Path d22 = new Path(d2, "subdir2");
    Path f221 = new Path(d22, "f1");
    fs.createNewFile(f221);

    Path d3 = new Path(USER_DIR, "dir3");
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
    Path d4 = new Path(USER_DIR, "dir4");
    fs.mkdirs(d4);

    /*
     * basic 
     */
    Path root = new Path(USER_DIR);
    status = fs.globStatus(root);
    checkStatus(status, root);
    
    status = fs.globStatus(new Path(USER_DIR, "x"));
    assertNull(status);

    status = fs.globStatus(new Path("x"));
    assertNull(status);

    status = fs.globStatus(new Path(USER_DIR, "x/x"));
    assertNull(status);

    status = fs.globStatus(new Path("x/x"));
    assertNull(status);

    status = fs.globStatus(new Path(USER_DIR, "*"));
    checkStatus(status, d1, d2, d3, d4);

    status = fs.globStatus(new Path("*"));
    checkStatus(status, d1, d2, d3, d4);

    status = fs.globStatus(new Path(USER_DIR, "*/x"));
    checkStatus(status);

    status = fs.globStatus(new Path("*/x"));
    checkStatus(status);

    status = fs.globStatus(new Path(USER_DIR, "x/*"));
    checkStatus(status);

    status = fs.globStatus(new Path("x/*"));
    checkStatus(status);

    // make sure full pattern is scanned instead of bailing early with undef
    status = fs.globStatus(new Path(USER_DIR, "x/x/x/*"));
    checkStatus(status);

    status = fs.globStatus(new Path("x/x/x/*"));
    checkStatus(status);

    status = fs.globStatus(new Path(USER_DIR, "*/*"));
    checkStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);

    status = fs.globStatus(new Path("*/*"));
    checkStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);

    /*
     * one level deep
     */
    status = fs.globStatus(new Path(USER_DIR, "dir*/*"));
    checkStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);

    status = fs.globStatus(new Path("dir*/*"));
    checkStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*"));
    checkStatus(status, d11, d12, d21, d22, f32, d33);

    status = fs.globStatus(new Path("dir*/subdir*"));
    checkStatus(status, d11, d12, d21, d22, f32, d33);

    status = fs.globStatus(new Path(USER_DIR, "dir*/f*"));
    checkStatus(status, f31, d32);

    status = fs.globStatus(new Path("dir*/f*"));
    checkStatus(status, f31, d32);

    /*
     * subdir1 globs
     */
    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1"));
    checkStatus(status, d11, d21);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/*"));
    checkStatus(status, f111, f112);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/*/*"));
    checkStatus(status);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/x"));
    checkStatus(status);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/x*"));
    checkStatus(status);

    /*
     * subdir2 globs
     */
    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir2"));
    checkStatus(status, d12, d22, f32);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir2/*"));
    checkStatus(status, f121, f221);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir2/*/*"));
    checkStatus(status);

    /*
     * subdir3 globs
     */
    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir3"));
    checkStatus(status, d33);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir3/*"));
    checkStatus(status, d331, f333); 

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir3/*/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir3/*/*/*"));
    checkStatus(status);

    /*
     * file1 single dir globs
     */    
    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/f1"));
    checkStatus(status, f111);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/f1*"));
    checkStatus(status, f111);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/f1/*"));
    checkStatus(status);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/f1*/*"));
    checkStatus(status);

    /*
     * file1 multi-dir globs
     */
    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f1"));
    checkStatus(status, f111, f121, f221, d331);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f1*"));
    checkStatus(status, f111, f121, f221, d331);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f1/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f1*/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f1*/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f1*/x"));
    checkStatus(status);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f1*/*/*"));
    checkStatus(status);

    /*
     *  file glob multiple files
     */

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*"));
    checkStatus(status, d11, d12, d21, d22, f32, d33);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/*"));
    checkStatus(status, f111, f112, f121, f221, d331, f333); 

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f*"));
    checkStatus(status, f111, f112, f121, f221, d331, f333);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/f*/*"));
    checkStatus(status, f3311);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/*/f1"));
    checkStatus(status, f3311); 

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir*/*/*"));
    checkStatus(status, f3311); 


    // doesn't exist
    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/f3"));
    checkStatus(status);

    status = fs.globStatus(new Path(USER_DIR, "dir*/subdir1/f3*"));
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
    checkStatus(status, new Path(USER_DIR));

    status = fs.globStatus(new Path(USER_DIR+"{/dir1}"));
    checkStatus(status, d1);

    status = fs.globStatus(new Path(USER_DIR+"{/dir*}"));
    checkStatus(status, d1, d2, d3, d4);

    /* 
     * true filter
     */

    PathFilter trueFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return true;
      }
    };

    status = fs.globStatus(new Path(Path.SEPARATOR), trueFilter);
    checkStatus(status, new Path(Path.SEPARATOR));
    
    status = fs.globStatus(new Path(Path.CUR_DIR), trueFilter);
    checkStatus(status, new Path(USER_DIR));    

    status = fs.globStatus(d1, trueFilter);
    checkStatus(status, d1);

    status = fs.globStatus(new Path(USER_DIR), trueFilter);
    checkStatus(status, new Path(USER_DIR));

    status = fs.globStatus(new Path(USER_DIR, "*"), trueFilter);
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
      public boolean accept(Path path) {
        return false;
      }
    };

    status = fs.globStatus(new Path(Path.SEPARATOR), falseFilter);
    assertNull(status);
    
    status = fs.globStatus(new Path(Path.CUR_DIR), falseFilter);
    assertNull(status);    
    
    status = fs.globStatus(new Path(USER_DIR), falseFilter);
    assertNull(status);
    
    status = fs.globStatus(new Path(USER_DIR, "*"), falseFilter);
    checkStatus(status);

    status = fs.globStatus(new Path("/x/*"), falseFilter);
    checkStatus(status);

    status = fs.globStatus(new Path("/x"), falseFilter);
    assertNull(status);

    status = fs.globStatus(new Path("/x/x"), falseFilter);
    assertNull(status);
  }
  
  private void checkStatus(FileStatus[] status, Path ... expectedMatches) {
    assertNotNull(status);
    String[] paths = new String[status.length];
    for (int i=0; i < status.length; i++) {
      paths[i] = getPathFromStatus(status[i]);
    }
    String got = StringUtils.join(paths, "\n");
    String expected = StringUtils.join(expectedMatches, "\n");
    assertEquals(expected, got);
  }

  private String getPathFromStatus(FileStatus status) {
    return status.getPath().toUri().getPath();
  }
  
  
  @Test
  public void testPathFilter() throws IOException {
    try {
      String[] files = new String[] { USER_DIR + "/a", USER_DIR + "/a/b" };
      Path[] matchedPath = prepareTesting(USER_DIR + "/*/*", files,
          new RegexPathFilter("^.*" + Pattern.quote(USER_DIR) + "/a/b"));
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[1]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void testPathFilterWithFixedLastComponent() throws IOException {
    try {
      String[] files = new String[] { USER_DIR + "/a", USER_DIR + "/a/b",
                                      USER_DIR + "/c", USER_DIR + "/c/b", };
      Path[] matchedPath = prepareTesting(USER_DIR + "/*/b", files,
          new RegexPathFilter("^.*" + Pattern.quote(USER_DIR) + "/a/b"));
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[1]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestLiteral() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/a2c", USER_DIR+"/abc.d"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/abc.d", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[1]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestEscape() throws IOException {
    // Skip the test case on Windows because backslash will be treated as a
    // path separator instead of an escaping character on Windows.
    org.junit.Assume.assumeTrue(!Path.WINDOWS);
    try {
      String [] files = new String[] {USER_DIR+"/ab\\[c.d"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/ab\\[c.d", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestAny() throws IOException {
    try {
      String [] files = new String[] { USER_DIR+"/abc", USER_DIR+"/a2c",
                                       USER_DIR+"/a.c", USER_DIR+"/abcd"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a?c", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[2]);
      assertEquals(matchedPath[1], path[1]);
      assertEquals(matchedPath[2], path[0]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestClosure1() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/a", USER_DIR+"/abc",
                                      USER_DIR+"/abc.p", USER_DIR+"/bacd"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a*", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);
      assertEquals(matchedPath[2], path[2]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestClosure2() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/a.", USER_DIR+"/a.txt",
                                     USER_DIR+"/a.old.java", USER_DIR+"/.java"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.*", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[2]);
      assertEquals(matchedPath[2], path[1]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestClosure3() throws IOException {
    try {    
      String [] files = new String[] {USER_DIR+"/a.txt.x", USER_DIR+"/ax",
                                      USER_DIR+"/ab37x", USER_DIR+"/bacd"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a*x", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[2]);
      assertEquals(matchedPath[2], path[1]);
    } finally {
      cleanupDFS();
    } 
  }

  @Test
  public void pTestClosure4() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/dir1/file1", 
                                      USER_DIR+"/dir2/file2", 
                                       USER_DIR+"/dir3/file1"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/*/file1", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[2]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestClosure5() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/dir1/file1", 
                                      USER_DIR+"/file1"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/*/file1", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
    } finally {
      cleanupDFS();
    }
  }

  @Test
  public void pTestSet() throws IOException {
    try {    
      String [] files = new String[] {USER_DIR+"/a.c", USER_DIR+"/a.cpp",
                                      USER_DIR+"/a.hlp", USER_DIR+"/a.hxy"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[ch]??", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[1]);
      assertEquals(matchedPath[1], path[2]);
      assertEquals(matchedPath[2], path[3]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestRange() throws IOException {
    try {    
      String [] files = new String[] {USER_DIR+"/a.d", USER_DIR+"/a.e",
                                      USER_DIR+"/a.f", USER_DIR+"/a.h"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[d-fm]", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);
      assertEquals(matchedPath[2], path[2]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestSetExcl() throws IOException {
    try {    
      String [] files = new String[] {USER_DIR+"/a.d", USER_DIR+"/a.e",
                                      USER_DIR+"/a.0", USER_DIR+"/a.h"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[^a-cg-z0-9]", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);
    } finally {
      cleanupDFS();
    }
  }

  @Test
  public void pTestCombination() throws IOException {
    try {    
      String [] files = new String[] {"/user/aa/a.c", "/user/bb/a.cpp",
                                      "/user1/cc/b.hlp", "/user/dd/a.hxy"};
      Path[] matchedPath = prepareTesting("/use?/*/a.[ch]{lp,xy}", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[3]);
    } finally {
      cleanupDFS();
    }
  }
  
  @Test
  public void pTestRelativePath() throws IOException {
    try {
      String [] files = new String[] {"a", "abc", "abc.p", "bacd"};
      Path[] matchedPath = prepareTesting("a*", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], new Path(USER_DIR, path[0]));
      assertEquals(matchedPath[1], new Path(USER_DIR, path[1]));
      assertEquals(matchedPath[2], new Path(USER_DIR, path[2]));
    } finally {
      cleanupDFS();
    }
  }
  
  /* Test {xx,yy} */
  @Test
  public void pTestCurlyBracket() throws IOException {
    Path[] matchedPath;
    String [] files;
    try {
      files = new String[] { USER_DIR+"/a.abcxx", USER_DIR+"/a.abxy",
                             USER_DIR+"/a.hlp", USER_DIR+"/a.jhyy"};
      matchedPath = prepareTesting(USER_DIR+"/a.{abc,jh}??", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[3]);
    } finally {
      cleanupDFS();
    }
    // nested curlies
    try {
      files = new String[] { USER_DIR+"/a.abcxx", USER_DIR+"/a.abdxy",
                             USER_DIR+"/a.hlp", USER_DIR+"/a.jhyy" };
      matchedPath = prepareTesting(USER_DIR+"/a.{ab{c,d},jh}??", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);
      assertEquals(matchedPath[2], path[3]);
    } finally {
      cleanupDFS();
    }
    // cross-component curlies
    try {
      files = new String[] { USER_DIR+"/a/b", USER_DIR+"/a/d",
                             USER_DIR+"/c/b", USER_DIR+"/c/d" };
      matchedPath = prepareTesting(USER_DIR+"/{a/b,c/d}", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[3]);
    } finally {
      cleanupDFS();
    }
    // cross-component absolute curlies
    try {
      files = new String[] { "/a/b", "/a/d",
                             "/c/b", "/c/d" };
      matchedPath = prepareTesting("{/a/b,/c/d}", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[3]);
    } finally {
      cleanupDFS();
    }
    try {
      // test standalone }
      files = new String[] {USER_DIR+"/}bc", USER_DIR+"/}c"};
      matchedPath = prepareTesting(USER_DIR+"/}{a,b}c", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
      // test {b}
      matchedPath = prepareTesting(USER_DIR+"/}{b}c", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
      // test {}
      matchedPath = prepareTesting(USER_DIR+"/}{}bc", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);

      // test {,}
      matchedPath = prepareTesting(USER_DIR+"/}{,}bc", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);

      // test {b,}
      matchedPath = prepareTesting(USER_DIR+"/}{b,}c", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);

      // test {,b}
      matchedPath = prepareTesting(USER_DIR+"/}{,b}c", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);

      // test a combination of {} and ?
      matchedPath = prepareTesting(USER_DIR+"/}{ac,?}", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[1]);
      
      // test ill-formed curly
      boolean hasException = false;
      try {
        prepareTesting(USER_DIR+"}{bc", files);
      } catch (IOException e) {
        assertTrue(e.getMessage().startsWith("Illegal file pattern:") );
        hasException = true;
      }
      assertTrue(hasException);
    } finally {
      cleanupDFS();
    }
  }
  
  /* test that a path name can contain Java regex special characters */
  @Test
  public void pTestJavaRegexSpecialChars() throws IOException {
    try {
      String[] files = new String[] {USER_DIR+"/($.|+)bc", USER_DIR+"/abc"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/($.|+)*", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
    } finally {
      cleanupDFS();
    }

  }
  
  private Path[] prepareTesting(String pattern, String[] files)
    throws IOException {
    for(int i=0; i<Math.min(NUM_OF_PATHS, files.length); i++) {
      path[i] = fs.makeQualified(new Path(files[i]));
      if (!fs.mkdirs(path[i])) {
        throw new IOException("Mkdirs failed to create " + path[i].toString());
      }
    }
    Path patternPath = new Path(pattern);
    Path[] globResults = FileUtil.stat2Paths(fs.globStatus(patternPath),
                                             patternPath);
    for(int i=0; i<globResults.length; i++) {
      globResults[i] = 
        globResults[i].makeQualified(fs.getUri(), fs.getWorkingDirectory());
    }
    return globResults;
  }
  
  private Path[] prepareTesting(String pattern, String[] files,
      PathFilter filter) throws IOException {
    for(int i=0; i<Math.min(NUM_OF_PATHS, files.length); i++) {
      path[i] = fs.makeQualified(new Path(files[i]));
      if (!fs.mkdirs(path[i])) {
        throw new IOException("Mkdirs failed to create " + path[i].toString());
      }
    }
    Path patternPath = new Path(pattern);
    Path[] globResults = FileUtil.stat2Paths(fs.globStatus(patternPath, filter),
                                             patternPath);
    for(int i=0; i<globResults.length; i++) {
      globResults[i] = 
        globResults[i].makeQualified(fs.getUri(), fs.getWorkingDirectory());
    }
    return globResults;
  }
  
  @After
  public void cleanupDFS() throws IOException {
    fs.delete(new Path(USER_DIR), true);
  }
  
}
