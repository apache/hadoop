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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests resolution of AbstractFileSystems for a given path with symlinks.
 */
public class TestFileContextResolveAfs {
  
  private static String TEST_ROOT_DIR_LOCAL
    = System.getProperty("test.build.data","/tmp");
  
  private FileContext fc;
  private FileSystem localFs;
  
  @Before
  public void setup() throws IOException {
    fc = FileContext.getFileContext();
  }
  
  @Test (timeout = 30000)
  public void testFileContextResolveAfs() throws IOException {
    Configuration conf = new Configuration();
    localFs = FileSystem.get(conf);
    
    Path localPath = new Path(TEST_ROOT_DIR_LOCAL + "/TestFileContextResolveAfs1");
    Path linkPath = localFs.makeQualified(new Path(TEST_ROOT_DIR_LOCAL,
      "TestFileContextResolveAfs2"));
    localFs.mkdirs(new Path(TEST_ROOT_DIR_LOCAL));
    localFs.create(localPath);
    
    fc.createSymlink(localPath, linkPath, true);
    Set<AbstractFileSystem> afsList = fc.resolveAbstractFileSystems(linkPath);
    Assert.assertEquals(1, afsList.size());
    localFs.deleteOnExit(localPath);
    localFs.deleteOnExit(linkPath);
    localFs.close();
  }
}
