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
import java.util.EnumSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.fs.Options.CreateOpts;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link FileContext.#deleteOnExit(Path)} functionality.
 */
public class TestFileContextDeleteOnExit {
  private static String TEST_ROOT_DIR =
    System.getProperty("test.build.data", "/tmp") + "/test";
  
  private static byte[] data = new byte[1024 * 2]; // two blocks of data
  {
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }
  }
  
  private FileContext fc;
  
  @Before
  public void setup() throws IOException {
    fc = FileContext.getLocalFSFileContext();
  }
  
  @After
  public void tearDown() throws IOException {
    fc.delete(getTestRootPath(), true);
  }
  
  private Path getTestRootPath() {
    return fc.makeQualified(new Path(TEST_ROOT_DIR));
  }
  
  private Path getTestPath(String pathString) {
    return fc.makeQualified(new Path(TEST_ROOT_DIR, pathString));
  }
  
  private void createFile(FileContext fc, Path path) throws IOException {
    FSDataOutputStream out = fc.create(path, EnumSet.of(CreateFlag.CREATE),
        CreateOpts.createParent());
    out.write(data, 0, data.length);
    out.close();
  }
  
  private void checkDeleteOnExitData(int size, FileContext fc, Path... paths) {
    Assert.assertEquals(size, FileContext.deleteOnExit.size());
    Set<Path> set = FileContext.deleteOnExit.get(fc);
    Assert.assertEquals(paths.length, (set == null ? 0 : set.size()));
    for (Path path : paths) {
      Assert.assertTrue(set.contains(path));
    }
  }
  
  @Test
  public void testDeleteOnExit() throws Exception {
    // Create deleteOnExit entries
    Path file1 = getTestPath("file1");
    createFile(fc, file1);
    fc.deleteOnExit(file1);
    checkDeleteOnExitData(1, fc, file1);
    
    // Ensure shutdown hook is added
    Assert.assertTrue(Runtime.getRuntime().removeShutdownHook(FileContext.finalizer));
    
    Path file2 = getTestPath("dir1/file2");
    createFile(fc, file2);
    fc.deleteOnExit(file2);
    checkDeleteOnExitData(1, fc, file1, file2);
    
    Path dir = getTestPath("dir3/dir4/dir5/dir6");
    createFile(fc, dir);
    fc.deleteOnExit(dir);
    checkDeleteOnExitData(1, fc, file1, file2, dir);
    
    // trigger deleteOnExit and ensure the registered
    // paths are cleaned up
    FileContext.finalizer.start();
    FileContext.finalizer.join();
    checkDeleteOnExitData(0, fc, new Path[0]);
    Assert.assertFalse(fc.exists(file1));
    Assert.assertFalse(fc.exists(file2));
    Assert.assertFalse(fc.exists(dir));
  }
}
