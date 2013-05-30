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

import junit.framework.Assert;
import org.apache.hadoop.util.ShutdownHookManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.FileContextTestHelper.*;

/**
 * Tests {@link FileContext.#deleteOnExit(Path)} functionality.
 */
public class TestFileContextDeleteOnExit {
  private static int blockSize = 1024;
  private static int numBlocks = 2;
  
  private final FileContextTestHelper helper = new FileContextTestHelper();
  private FileContext fc;
  
  @Before
  public void setup() throws IOException {
    fc = FileContext.getLocalFSFileContext();
  }
  
  @After
  public void tearDown() throws IOException {
    fc.delete(helper.getTestRootPath(fc), true);
  }
  
  
  private void checkDeleteOnExitData(int size, FileContext fc, Path... paths) {
    Assert.assertEquals(size, FileContext.DELETE_ON_EXIT.size());
    Set<Path> set = FileContext.DELETE_ON_EXIT.get(fc);
    Assert.assertEquals(paths.length, (set == null ? 0 : set.size()));
    for (Path path : paths) {
      Assert.assertTrue(set.contains(path));
    }
  }
  
  @Test
  public void testDeleteOnExit() throws Exception {
    // Create deleteOnExit entries
    Path file1 = helper.getTestRootPath(fc, "file1");
    createFile(fc, file1, numBlocks, blockSize);
    fc.deleteOnExit(file1);
    checkDeleteOnExitData(1, fc, file1);
    
    // Ensure shutdown hook is added
    Assert.assertTrue(ShutdownHookManager.get().hasShutdownHook(FileContext.FINALIZER));
    
    Path file2 = helper.getTestRootPath(fc, "dir1/file2");
    createFile(fc, file2, numBlocks, blockSize);
    fc.deleteOnExit(file2);
    checkDeleteOnExitData(1, fc, file1, file2);
    
    Path dir = helper.getTestRootPath(fc, "dir3/dir4/dir5/dir6");
    createFile(fc, dir, numBlocks, blockSize);
    fc.deleteOnExit(dir);
    checkDeleteOnExitData(1, fc, file1, file2, dir);
    
    // trigger deleteOnExit and ensure the registered
    // paths are cleaned up
    FileContext.FINALIZER.run();
    checkDeleteOnExitData(0, fc, new Path[0]);
    Assert.assertFalse(exists(fc, file1));
    Assert.assertFalse(exists(fc, file2));
    Assert.assertFalse(exists(fc, dir));
  }
}
