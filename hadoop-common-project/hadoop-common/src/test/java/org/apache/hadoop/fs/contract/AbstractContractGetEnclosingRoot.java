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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractContractGetEnclosingRoot extends AbstractFSContractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContractGetEnclosingRoot.class);

  @Test
  public void testEnclosingRootEquivalence() throws IOException {
    FileSystem fs = getFileSystem();
    Path root = path("/");
    Path foobar = path("/foo/bar");

    assertEquals(root, fs.getEnclosingRoot(foobar),
        "Ensure getEnclosingRoot on the root directory returns the root directory");
    assertEquals(root, fs.getEnclosingRoot(fs.getEnclosingRoot(foobar)),
        "Ensure getEnclosingRoot called on itself returns the root directory");
    assertEquals(fs.getEnclosingRoot(root), fs.getEnclosingRoot(foobar),
        "Ensure getEnclosingRoot for different paths in the same enclosing root "
        + "returns the same path");
    assertEquals(root, fs.getEnclosingRoot(methodPath()),
        "Ensure getEnclosingRoot on a path returns the root directory");
    assertEquals(root, fs.getEnclosingRoot(fs.getEnclosingRoot(methodPath())),
        "Ensure getEnclosingRoot called on itself on a path returns the root directory");
    assertEquals(fs.getEnclosingRoot(root), fs.getEnclosingRoot(methodPath()),
        "Ensure getEnclosingRoot for different paths in the same enclosing root "
        + "returns the same path");
  }


  @Test
  public void testEnclosingRootPathExists() throws Exception {
    FileSystem fs = getFileSystem();
    Path root = path("/");
    Path foobar = methodPath();
    fs.mkdirs(foobar);

    assertEquals(root, fs.getEnclosingRoot(foobar),
        "Ensure getEnclosingRoot returns the root directory " +
        "when the root directory exists");
    assertEquals(root, fs.getEnclosingRoot(foobar),
        "Ensure getEnclosingRoot returns the root directory " +
        "when the directory exists");
  }

  @Test
  public void testEnclosingRootPathDNE() throws Exception {
    FileSystem fs = getFileSystem();
    Path foobar = path("/foo/bar");
    Path root = path("/");

    // .
    assertEquals(root, fs.getEnclosingRoot(foobar),
        "Ensure getEnclosingRoot returns the root directory " +
        "even when the path does not exist");
    assertEquals(root, fs.getEnclosingRoot(methodPath()),
        "Ensure getEnclosingRoot returns the root directory " +
        "even when the path does not exist");
  }

  @Test
  public void testEnclosingRootWrapped() throws Exception {
    FileSystem fs = getFileSystem();
    Path root = path("/");

    assertEquals(root, fs.getEnclosingRoot(new Path("/foo/bar")),
        "Ensure getEnclosingRoot returns the root directory " +
        "when the directory exists");

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("foo");
    Path p = ugi.doAs((PrivilegedExceptionAction<Path>) () -> {
      FileSystem wFs = getContract().getTestFileSystem();
      return wFs.getEnclosingRoot(new Path("/foo/bar"));
    });
    assertEquals(root, p, "Ensure getEnclosingRoot works correctly " +
        "within a wrapped FileSystem");
  }
}
