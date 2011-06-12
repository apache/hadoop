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
package org.apache.hadoop.mapreduce.util;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;
import org.junit.Test;

/**
 * A test for MRAsyncDiskService.
 */
public class TestMRAsyncDiskService extends TestCase {
  
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString();

  /**
   * This test creates some directories and then removes them through 
   * MRAsyncDiskService. 
   */
  @Test
  public void testMRAsyncDiskService() throws Throwable {
  
    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String[] vols = new String[]{TEST_ROOT_DIR + "/0",
        TEST_ROOT_DIR + "/1"};
    MRAsyncDiskService service = new MRAsyncDiskService(
        localFileSystem, vols);
    
    String a = "a";
    String b = "b";
    String c = "b/c";
    String d = "d";
    
    File fa = new File(vols[0], a);
    File fb = new File(vols[1], b);
    File fc = new File(vols[1], c);
    File fd = new File(vols[1], d);
    
    // Create the directories
    fa.mkdirs();
    fb.mkdirs();
    fc.mkdirs();
    fd.mkdirs();
    
    assertTrue(fa.exists());
    assertTrue(fb.exists());
    assertTrue(fc.exists());
    assertTrue(fd.exists());
    
    // Move and delete them
    service.moveAndDeleteRelativePath(vols[0], a);
    assertFalse(fa.exists());
    service.moveAndDeleteRelativePath(vols[1], b);
    assertFalse(fb.exists());
    assertFalse(fc.exists());
    
    // asyncDiskService is NOT able to delete files outside all volumes.
    IOException ee = null;
    try {
      service.moveAndDeleteAbsolutePath(TEST_ROOT_DIR + "/2");
    } catch (IOException e) {
      ee = e;
    }
    assertNotNull("asyncDiskService should not be able to delete files "
        + "outside all volumes", ee);
    // asyncDiskService is able to automatically find the file in one
    // of the volumes.
    assertTrue(service.moveAndDeleteAbsolutePath(vols[1] + Path.SEPARATOR_CHAR + d));
    
    // Make sure everything is cleaned up
    makeSureCleanedUp(vols, service);
  }

  /**
   * This test creates some directories inside the volume roots, and then 
   * call asyncDiskService.MoveAndDeleteAllVolumes.
   * We should be able to delete all files/dirs inside the volumes except
   * the toBeDeleted directory.
   */
  @Test
  public void testMRAsyncDiskServiceMoveAndDeleteAllVolumes() throws Throwable {
    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String[] vols = new String[]{TEST_ROOT_DIR + "/0",
        TEST_ROOT_DIR + "/1"};
    MRAsyncDiskService service = new MRAsyncDiskService(
        localFileSystem, vols);

    String a = "a";
    String b = "b";
    String c = "b/c";
    String d = "d";
    
    File fa = new File(vols[0], a);
    File fb = new File(vols[1], b);
    File fc = new File(vols[1], c);
    File fd = new File(vols[1], d);
    
    // Create the directories
    fa.mkdirs();
    fb.mkdirs();
    fc.mkdirs();
    fd.mkdirs();

    assertTrue(fa.exists());
    assertTrue(fb.exists());
    assertTrue(fc.exists());
    assertTrue(fd.exists());
    
    // Delete all of them
    service.cleanupAllVolumes();
    
    assertFalse(fa.exists());
    assertFalse(fb.exists());
    assertFalse(fc.exists());
    assertFalse(fd.exists());
    
    // Make sure everything is cleaned up
    makeSureCleanedUp(vols, service);
  }
  
  /**
   * This test creates some directories inside the toBeDeleted directory and
   * then start the asyncDiskService.
   * AsyncDiskService will create tasks to delete the content inside the
   * toBeDeleted directories.
   */
  @Test
  public void testMRAsyncDiskServiceStartupCleaning() throws Throwable {
    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String[] vols = new String[]{TEST_ROOT_DIR + "/0",
        TEST_ROOT_DIR + "/1"};

    String a = "a";
    String b = "b";
    String c = "b/c";
    String d = "d";
    
    // Create directories inside SUBDIR
    File fa = new File(vols[0] + Path.SEPARATOR_CHAR + MRAsyncDiskService.TOBEDELETED, a);
    File fb = new File(vols[1] + Path.SEPARATOR_CHAR + MRAsyncDiskService.TOBEDELETED, b);
    File fc = new File(vols[1] + Path.SEPARATOR_CHAR + MRAsyncDiskService.TOBEDELETED, c);
    File fd = new File(vols[1] + Path.SEPARATOR_CHAR + MRAsyncDiskService.TOBEDELETED, d);
    
    // Create the directories
    fa.mkdirs();
    fb.mkdirs();
    fc.mkdirs();
    fd.mkdirs();

    assertTrue(fa.exists());
    assertTrue(fb.exists());
    assertTrue(fc.exists());
    assertTrue(fd.exists());
    
    // Create the asyncDiskService which will delete all contents inside SUBDIR
    MRAsyncDiskService service = new MRAsyncDiskService(
        localFileSystem, vols);
    
    // Make sure everything is cleaned up
    makeSureCleanedUp(vols, service);
  }
  
  private void makeSureCleanedUp(String[] vols, MRAsyncDiskService service)
      throws Throwable {
    // Sleep at most 5 seconds to make sure the deleted items are all gone.
    service.shutdown();
    if (!service.awaitTermination(5000)) {
      fail("MRAsyncDiskService is still not shutdown in 5 seconds!");
    }
    
    // All contents should be gone by now.
    for (int i = 0; i < vols.length; i++) {
      File subDir = new File(vols[0]);
      String[] subDirContent = subDir.list();
      assertEquals("Volume should contain a single child: "
          + MRAsyncDiskService.TOBEDELETED, 1, subDirContent.length);
      
      File toBeDeletedDir = new File(vols[0], MRAsyncDiskService.TOBEDELETED);
      String[] content = toBeDeletedDir.list();
      assertNotNull("Cannot find " + toBeDeletedDir, content);
      assertEquals("" + toBeDeletedDir + " should be empty now.", 0,
          content.length);
    }
  }
    
}
