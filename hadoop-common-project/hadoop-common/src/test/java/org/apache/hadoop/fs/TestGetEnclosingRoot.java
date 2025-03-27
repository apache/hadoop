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
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.HadoopTestBase;
import org.junit.Test;

public class TestGetEnclosingRoot extends HadoopTestBase {
  @Test
  public void testEnclosingRootEquivalence() throws IOException {
    FileSystem fs = getFileSystem();
    Path root = path("/");
    Path foobar = path("/foo/bar");

    assertEquals(root, fs.getEnclosingRoot(root));
    assertEquals(root, fs.getEnclosingRoot(foobar));
    assertEquals(root, fs.getEnclosingRoot(fs.getEnclosingRoot(foobar)));
    assertEquals(fs.getEnclosingRoot(root), fs.getEnclosingRoot(foobar));

    assertEquals(root, fs.getEnclosingRoot(path(foobar.toString())));
    assertEquals(root, fs.getEnclosingRoot(fs.getEnclosingRoot(path(foobar.toString()))));
    assertEquals(fs.getEnclosingRoot(root), fs.getEnclosingRoot(path(foobar.toString())));
  }

  @Test
  public void testEnclosingRootPathExists() throws Exception {
    FileSystem fs = getFileSystem();
    Path root = path("/");
    Path foobar = path("/foo/bar");
    fs.mkdirs(foobar);

    assertEquals(root, fs.getEnclosingRoot(foobar));
    assertEquals(root, fs.getEnclosingRoot(path(foobar.toString())));
  }

  @Test
  public void testEnclosingRootPathDNE() throws Exception {
    FileSystem fs = getFileSystem();
    Path foobar = path("/foo/bar");
    Path root = path("/");

    assertEquals(root, fs.getEnclosingRoot(foobar));
    assertEquals(root, fs.getEnclosingRoot(path(foobar.toString())));
  }

  @Test
  public void testEnclosingRootWrapped() throws Exception {
    FileSystem fs = getFileSystem();
    Path root = path("/");

    assertEquals(root, fs.getEnclosingRoot(new Path("/foo/bar")));

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("foo");
    Path p = ugi.doAs((PrivilegedExceptionAction<Path>) () -> {
      FileSystem wFs = getFileSystem();
      return wFs.getEnclosingRoot(new Path("/foo/bar"));
    });
    assertEquals(root, p);
  }

  private FileSystem getFileSystem() throws IOException {
    return FileSystem.get(new Configuration());
  }

  /**
   * Create a path under the test path provided by
   * the FS contract.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  private Path path(String filepath) throws IOException {
    return getFileSystem().makeQualified(
        new Path(filepath));
  }}
