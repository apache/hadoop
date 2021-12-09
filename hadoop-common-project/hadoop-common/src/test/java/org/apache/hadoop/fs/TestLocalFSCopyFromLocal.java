/*
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

import java.io.File;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCopyFromLocalTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;

public class TestLocalFSCopyFromLocal extends AbstractContractCopyFromLocalTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LocalFSContract(conf);
  }

  @Test
  public void testDestinationFileIsToParentDirectory() throws Throwable {
    describe("Source is a file and destination is its own parent directory. " +
        "Copying will cause the source file to be deleted.");

    File file = createTempFile("local");
    Path dest = new Path(file.getParentFile().toURI());
    Path src = new Path(file.toURI());

    getFileSystem().copyFromLocalFile(true, true, src, dest);
    assertPathDoesNotExist("Source found", src);
  }

  @Test
  public void testDestinationDirectoryToSelf() throws Throwable {
    describe("Source is a directory and it is copied into itself with " +
        "delSrc flag set, destination must not exist");

    File source = createTempDirectory("srcDir");
    Path dest = new Path(source.toURI());
    getFileSystem().copyFromLocalFile( true, true, dest, dest);

    assertPathDoesNotExist("Source found", dest);
  }

  @Test
  public void testSourceIntoDestinationSubDirectoryWithDelSrc() throws Throwable {
    describe("Copying a parent folder inside a child folder with" +
        " delSrc=TRUE");
    File parent = createTempDirectory("parent");
    File child = createTempDirectory(parent, "child");

    Path src = new Path(parent.toURI());
    Path dest = new Path(child.toURI());
    getFileSystem().copyFromLocalFile(true, true, src, dest);

    assertPathDoesNotExist("Source found", src);
    assertPathDoesNotExist("Destination found", dest);
  }

  @Test
  public void testSourceIntoDestinationSubDirectory() throws Throwable {
    describe("Copying a parent folder inside a child folder with" +
        " delSrc=FALSE");
    File parent = createTempDirectory("parent");
    File child = createTempDirectory(parent, "child");

    Path src = new Path(parent.toURI());
    Path dest = new Path(child.toURI());
    getFileSystem().copyFromLocalFile(false, true, src, dest);

    Path recursiveParent = new Path(dest, parent.getName());
    Path recursiveChild = new Path(recursiveParent, child.getName());

    // This definitely counts as interesting behaviour which needs documented
    // Depending on the underlying system this can recurse 15+ times
    recursiveParent = new Path(recursiveChild, parent.getName());
    recursiveChild = new Path(recursiveParent, child.getName());
    assertPathExists("Recursive parent not found", recursiveParent);
    assertPathExists("Recursive child not found", recursiveChild);
  }
}
