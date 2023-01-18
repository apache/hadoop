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
package org.apache.hadoop.fs.contract;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractContractGetEnclosingRoot extends AbstractFSContractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContractGetEnclosingRoot.class);

  @Override
  public void setup() throws Exception {
    super.setup();
  }

  @Test
  public void testEnclosingRootEquivalence() throws IOException {
    FileSystem fs = getFileSystem();
    Path root = path("/");
    Path foobar = path("/foo/bar");

    assertEquals(fs.getEnclosingRoot(foobar), root);
    assertEquals(fs.getEnclosingRoot(fs.getEnclosingRoot(foobar)), root);
    assertEquals(fs.getEnclosingRoot(foobar), fs.getEnclosingRoot(root));

    assertEquals(fs.getEnclosingRoot(path(foobar.toString())), root);
    assertEquals(fs.getEnclosingRoot(fs.getEnclosingRoot(path(foobar.toString()))), root);
    assertEquals(fs.getEnclosingRoot(path(foobar.toString())), fs.getEnclosingRoot(root));
  }

  @Test
  public void testEnclosingRootPathExists() throws Exception {
    FileSystem fs = getFileSystem();
    Path root = path("/");
    Path foobar = path("/foo/bar");
    fs.mkdirs(foobar);

    assertEquals(fs.getEnclosingRoot(foobar), root);
    assertEquals(fs.getEnclosingRoot(path(foobar.toString())), root);
  }

  @Test
  public void testEnclosingRootPathDNE() throws Exception {
    FileSystem fs = getFileSystem();
    Path foobar = path("/foo/bar");
    Path root = path("/");

    assertEquals(fs.getEnclosingRoot(foobar), root);
    assertEquals(fs.getEnclosingRoot(path(foobar.toString())), root);
  }

  @Test
  public void testEnclosingRootWrapped() throws Exception {
    FileSystem fs = getFileSystem();
    Path root = path("/");

    assertEquals(fs.getEnclosingRoot(new Path("/foo/bar")), root);

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("foo");
    Path p = ugi.doAs((PrivilegedExceptionAction<Path>) () -> {
      FileSystem wFs = getContract().getTestFileSystem();
      return wFs.getEnclosingRoot(new Path("/foo/bar"));
    });
    assertEquals(p, root);
  }

  /**
   * Create a path under the test path provided by
   * the FS contract.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  protected Path path(String filepath) throws IOException {
    return getFileSystem().makeQualified(
        new Path(getContract().getTestPath(), filepath));
  }
}