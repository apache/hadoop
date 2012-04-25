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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.fs.FileUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDirectoryCollection {

  private static final File testDir = new File("target",
      TestDirectoryCollection.class.getName()).getAbsoluteFile();
  private static final File testFile = new File(testDir, "testfile");

  @BeforeClass
  public static void setup() throws IOException {
    testDir.mkdirs();
    testFile.createNewFile();
  }

  @AfterClass
  public static void teardown() {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testConcurrentAccess() throws IOException {
    // Initialize DirectoryCollection with a file instead of a directory
    String[] dirs = {testFile.getPath()};
    DirectoryCollection dc = new DirectoryCollection(dirs);

    // Create an iterator before checkDirs is called to reliable test case
    List<String> list = dc.getGoodDirs();
    ListIterator<String> li = list.listIterator();

    // DiskErrorException will invalidate iterator of non-concurrent
    // collections. ConcurrentModificationException will be thrown upon next
    // use of the iterator.
    Assert.assertTrue("checkDirs did not remove test file from directory list",
        dc.checkDirs());

    // Verify no ConcurrentModification is thrown
    li.next();
  }
}
