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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;

import junit.framework.TestCase;

public class TestGlobPaths extends TestCase {
  
  static private MiniDFSCluster dfsCluster;
  static private FileSystem fs;
  static final private int NUM_OF_PATHS = 4;
  static final String USER_DIR = "/user/"+System.getProperty("user.name");
  private Path[] path = new Path[NUM_OF_PATHS];
  
  protected void setUp() throws Exception {
    try {
      Configuration conf = new Configuration();
      dfsCluster = new MiniDFSCluster(8889, conf, true);
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  protected void tearDown() throws Exception {
    dfsCluster.shutdown();
  }
  
  public void testGlob() {
    try {
      pTestLiteral();
      pTestAny();
      pTestClosure();
      pTestSet();
      pTestRange();
      pTestSetExcl();
      pTestCombination();
      pTestRelativePath();
    } catch( IOException e) {
      e.printStackTrace();
    } 
  }
  
  private void pTestLiteral() throws IOException {
    try {
      String [] files = new String[2];
      files[0] = USER_DIR+"/a2c";
      files[1] = USER_DIR+"/ab\\[c.d";
      Path[] matchedPath = prepareTesting( USER_DIR+"/ab\\[c.d", files );
      assertEquals( matchedPath.length, 1 );
      assertEquals( matchedPath[0], path[1] );
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestAny() throws IOException {
    try {
      String [] files = new String[4];
      files[0] = USER_DIR+"/abc";
      files[1] = USER_DIR+"/a2c";
      files[2] = USER_DIR+"/a.c";
      files[3] = USER_DIR+"/abcd";
      Path[] matchedPath = prepareTesting(USER_DIR+"/a?c", files);
      assertEquals( matchedPath.length, 3 );
      assertEquals( matchedPath[0], path[2] );
      assertEquals( matchedPath[1], path[1] );
      assertEquals( matchedPath[2], path[0] );
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestClosure() throws IOException {
    pTestClosure1();
    pTestClosure2();
    pTestClosure3();
  }
  
  private void pTestClosure1() throws IOException {
    try {
      String [] files = new String[4];
      files[0] = USER_DIR+"/a";
      files[1] = USER_DIR+"/abc";
      files[2] = USER_DIR+"/abc.p";
      files[3] = USER_DIR+"/bacd";
      Path[] matchedPath = prepareTesting(USER_DIR+"/a*", files);
      assertEquals( matchedPath.length, 3 );
      assertEquals( matchedPath[0], path[0] );
      assertEquals( matchedPath[1], path[1] );
      assertEquals( matchedPath[2], path[2] );
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestClosure2() throws IOException {
    try {
      String [] files = new String[4];
      files[0] = USER_DIR+"/a.";
      files[1] = USER_DIR+"/a.txt";
      files[2] = USER_DIR+"/a.old.java";
      files[3] = USER_DIR+"/.java";
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.*", files);
      assertEquals( matchedPath.length, 3 );
      assertEquals( matchedPath[0], path[0] );
      assertEquals( matchedPath[1], path[2] );
      assertEquals( matchedPath[2], path[1] );
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestClosure3() throws IOException {
    try {    
      String [] files = new String[4];
      files[0] = USER_DIR+"/a.txt.x";
      files[1] = USER_DIR+"/ax";
      files[2] = USER_DIR+"/ab37x";
      files[3] = USER_DIR+"/bacd";
      Path[] matchedPath = prepareTesting(USER_DIR+"/a*x", files);
      assertEquals( matchedPath.length, 3 );
      assertEquals( matchedPath[0], path[0] );
      assertEquals( matchedPath[1], path[2] );
      assertEquals( matchedPath[2], path[1] );
    } finally {
      cleanupDFS();
    } 
  }
  
  private void pTestSet() throws IOException {
    try {    
      String [] files = new String[4];
      files[0] = USER_DIR+"/a.c";
      files[1] = USER_DIR+"/a.cpp";
      files[2] = USER_DIR+"/a.hlp";
      files[3] = USER_DIR+"/a.hxy";
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[ch]??", files);
      assertEquals( matchedPath.length, 3 );
      assertEquals( matchedPath[0], path[1] );
      assertEquals( matchedPath[1], path[2] );
      assertEquals( matchedPath[2], path[3] );
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestRange() throws IOException {
    try {    
      String [] files = new String[4];
      files[0] = USER_DIR+"/a.d";
      files[1] = USER_DIR+"/a.e";
      files[2] = USER_DIR+"/a.f";
      files[3] = USER_DIR+"/a.h";
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[d-fm]", files);
      assertEquals( matchedPath.length, 3 );
      assertEquals( matchedPath[0], path[0] );
      assertEquals( matchedPath[1], path[1] );
      assertEquals( matchedPath[2], path[2] );
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestSetExcl() throws IOException {
    try {    
      String [] files = new String[4];
      files[0] = USER_DIR+"/a.d";
      files[1] = USER_DIR+"/a.e";
      files[2] = USER_DIR+"/a.0";
      files[3] = USER_DIR+"/a.h";
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[^a-cg-z0-9]", files);
      assertEquals( matchedPath.length, 2 );
      assertEquals( matchedPath[0], path[0] );
      assertEquals( matchedPath[1], path[1] );
    } finally {
      cleanupDFS();
    }
  }

  private void pTestCombination() throws IOException {
    try {    
      String [] files = new String[4];
      files[0] = "/user/aa/a.c";
      files[1] = "/user/bb/a.cpp";
      files[2] = "/user1/cc/b.hlp";
      files[3] = "/user/dd/a.hxy";
      Path[] matchedPath = prepareTesting("/use?/*/a.[ch]??", files);
      assertEquals( matchedPath.length, 2 );
      assertEquals( matchedPath[0], path[1] );
      assertEquals( matchedPath[1], path[3] );
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestRelativePath() throws IOException {
    try {
      String [] files = new String[4];
      files[0] = "a";
      files[1] = "abc";
      files[2] = "abc.p";
      files[3] = "bacd";
      Path[] matchedPath = prepareTesting("a*", files);
      assertEquals( matchedPath.length, 3 );
      assertEquals( matchedPath[0], new Path(USER_DIR, path[0]) );
      assertEquals( matchedPath[1], new Path(USER_DIR, path[1]) );
      assertEquals( matchedPath[2], new Path(USER_DIR, path[2]) );
    } finally {
      cleanupDFS();
    }
  }
  
  private Path[] prepareTesting( String pattern, String[] files)
  throws IOException {
    for(int i=0; i<Math.min(NUM_OF_PATHS, files.length); i++) {
      path[i] = new Path( files[i] );
      if (!fs.mkdirs( path[i] )) {
        throw new IOException("Mkdirs failed to create " + path[i].toString());
      }
    }
    return fs.globPaths( new Path(pattern) );
  }
  
  private void cleanupDFS( ) throws IOException {
    fs.delete( new Path("/user"));
  }
  
}
