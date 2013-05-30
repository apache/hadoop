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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFSMainOperationsLocalFileSystem extends FSMainOperationsBaseTest {

  @Override
  protected FileSystem createFileSystem() throws IOException {
    return FileSystem.getLocal(new Configuration());
  }
    
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }
  
  static Path wd = null;
  @Override
  protected Path getDefaultWorkingDirectory() throws IOException {
    if (wd == null)
      wd = FileSystem.getLocal(new Configuration()).getWorkingDirectory();
    return wd;
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @Test
  @Override
  public void testWDAbsolute() throws IOException {
    Path absoluteDir = getTestRootPath(fSys, "test/existingDir");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());
  }
}
