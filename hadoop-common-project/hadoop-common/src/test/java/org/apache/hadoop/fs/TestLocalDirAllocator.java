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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

/** This test LocalDirAllocator works correctly;
 * Every test case uses different buffer dirs to
 * enforce the AllocatorPerContext initialization.
 * This test does not run on Cygwin because under Cygwin
 * a directory can be created in a read-only directory
 * which breaks this test.
 */
@RunWith(Parameterized.class)
public class TestLocalDirAllocator {
  final static private Configuration conf = new Configuration();
  final static private String BUFFER_DIR_ROOT = "build/test/temp";
  final static private String ABSOLUTE_DIR_ROOT;
  final static private String QUALIFIED_DIR_ROOT;
  final static private Path BUFFER_PATH_ROOT = new Path(BUFFER_DIR_ROOT);
  final static private File BUFFER_ROOT = new File(BUFFER_DIR_ROOT);
  final static private String CONTEXT = "mapred.local.dir";
  final static private String FILENAME = "block";
  final static private LocalDirAllocator dirAllocator =
    new LocalDirAllocator(CONTEXT);
  static LocalFileSystem localFs;
  final static private boolean isWindows =
    System.getProperty("os.name").startsWith("Windows");
  final static int SMALL_FILE_SIZE = 100;
  final static private String RELATIVE = "/RELATIVE";
  final static private String ABSOLUTE = "/ABSOLUTE";
  final static private String QUALIFIED = "/QUALIFIED";
  final private String ROOT;
  final private String PREFIX;

  static {
    try {
      localFs = FileSystem.getLocal(conf);
      rmBufferDirs();
    } catch(IOException e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    // absolute path in test environment
    // /home/testuser/src/hadoop-common-project/hadoop-common/build/test/temp
    ABSOLUTE_DIR_ROOT = new Path(localFs.getWorkingDirectory(),
        BUFFER_DIR_ROOT).toUri().getPath();
    // file:/home/testuser/src/hadoop-common-project/hadoop-common/build/test/temp
    QUALIFIED_DIR_ROOT = new Path(localFs.getWorkingDirectory(),
        BUFFER_DIR_ROOT).toUri().toString();
  }

  public TestLocalDirAllocator(String root, String prefix) {
    ROOT = root;
    PREFIX = prefix;
  }

  @Parameters
  public static Collection<Object[]> params() {
    Object [][] data = new Object[][] {
      { BUFFER_DIR_ROOT, RELATIVE },
      { ABSOLUTE_DIR_ROOT, ABSOLUTE },
      { QUALIFIED_DIR_ROOT, QUALIFIED }
    };

    return Arrays.asList(data);
  }

  private static void rmBufferDirs() throws IOException {
    assertTrue(!localFs.exists(BUFFER_PATH_ROOT) ||
        localFs.delete(BUFFER_PATH_ROOT, true));
  }

  private static void validateTempDirCreation(String dir) throws IOException {
    File result = createTempFile(SMALL_FILE_SIZE);
    assertTrue("Checking for " + dir + " in " + result + " - FAILED!",
        result.getPath().startsWith(new Path(dir, FILENAME).toUri().getPath()));
  }

  private static File createTempFile() throws IOException {
    return createTempFile(-1);
  }

  private static File createTempFile(long size) throws IOException {
    File result = dirAllocator.createTmpFileForWrite(FILENAME, size, conf);
    result.delete();
    return result;
  }

  private String buildBufferDir(String dir, int i) {
    return dir + PREFIX + i;
  }

  /** Two buffer dirs. The first dir does not exist & is on a read-only disk;
   * The second dir exists & is RW
   * @throws Exception
   */
  @Test (timeout = 30000)
  public void test0() throws Exception {
    if (isWindows) return;
    String dir0 = buildBufferDir(ROOT, 0);
    String dir1 = buildBufferDir(ROOT, 1);
    try {
      conf.set(CONTEXT, dir0 + "," + dir1);
      assertTrue(localFs.mkdirs(new Path(dir1)));
      BUFFER_ROOT.setReadOnly();
      validateTempDirCreation(dir1);
      validateTempDirCreation(dir1);
    } finally {
      Shell.execCommand(Shell.getSetPermissionCommand("u+w", false,
                                                      BUFFER_DIR_ROOT));
      rmBufferDirs();
    }
  }

  /** Two buffer dirs. The first dir exists & is on a read-only disk;
   * The second dir exists & is RW
   * @throws Exception
   */
  @Test (timeout = 30000)
  public void testROBufferDirAndRWBufferDir() throws Exception {
    if (isWindows) return;
    String dir1 = buildBufferDir(ROOT, 1);
    String dir2 = buildBufferDir(ROOT, 2);
    try {
      conf.set(CONTEXT, dir1 + "," + dir2);
      assertTrue(localFs.mkdirs(new Path(dir2)));
      BUFFER_ROOT.setReadOnly();
      validateTempDirCreation(dir2);
      validateTempDirCreation(dir2);
    } finally {
      Shell.execCommand(Shell.getSetPermissionCommand("u+w", false,
                                                      BUFFER_DIR_ROOT));
      rmBufferDirs();
    }
  }
  /** Two buffer dirs. Both do not exist but on a RW disk.
   * Check if tmp dirs are allocated in a round-robin
   */
  @Test (timeout = 30000)
  public void testDirsNotExist() throws Exception {
    if (isWindows) return;
    String dir2 = buildBufferDir(ROOT, 2);
    String dir3 = buildBufferDir(ROOT, 3);
    try {
      conf.set(CONTEXT, dir2 + "," + dir3);

      // create the first file, and then figure the round-robin sequence
      createTempFile(SMALL_FILE_SIZE);
      int firstDirIdx = (dirAllocator.getCurrentDirectoryIndex() == 0) ? 2 : 3;
      int secondDirIdx = (firstDirIdx == 2) ? 3 : 2;

      // check if tmp dirs are allocated in a round-robin manner
      validateTempDirCreation(buildBufferDir(ROOT, firstDirIdx));
      validateTempDirCreation(buildBufferDir(ROOT, secondDirIdx));
      validateTempDirCreation(buildBufferDir(ROOT, firstDirIdx));
    } finally {
      rmBufferDirs();
    }
  }

  /** Two buffer dirs. Both exists and on a R/W disk.
   * Later disk1 becomes read-only.
   * @throws Exception
   */
  @Test (timeout = 30000)
  public void testRWBufferDirBecomesRO() throws Exception {
    if (isWindows) return;
    String dir3 = buildBufferDir(ROOT, 3);
    String dir4 = buildBufferDir(ROOT, 4);
    try {
      conf.set(CONTEXT, dir3 + "," + dir4);
      assertTrue(localFs.mkdirs(new Path(dir3)));
      assertTrue(localFs.mkdirs(new Path(dir4)));

      // Create the first small file
      createTempFile(SMALL_FILE_SIZE);

      // Determine the round-robin sequence
      int nextDirIdx = (dirAllocator.getCurrentDirectoryIndex() == 0) ? 3 : 4;
      validateTempDirCreation(buildBufferDir(ROOT, nextDirIdx));

      // change buffer directory 2 to be read only
      new File(new Path(dir4).toUri().getPath()).setReadOnly();
      validateTempDirCreation(dir3);
      validateTempDirCreation(dir3);
    } finally {
      rmBufferDirs();
    }
  }

  /**
   * Two buffer dirs, on read-write disk.
   *
   * Try to create a whole bunch of files.
   *  Verify that they do indeed all get created where they should.
   *
   *  Would ideally check statistical properties of distribution, but
   *  we don't have the nerve to risk false-positives here.
   *
   * @throws Exception
   */
  static final int TRIALS = 100;
  @Test (timeout = 30000)
  public void testCreateManyFiles() throws Exception {
    if (isWindows) return;
    String dir5 = buildBufferDir(ROOT, 5);
    String dir6 = buildBufferDir(ROOT, 6);
    try {

      conf.set(CONTEXT, dir5 + "," + dir6);
      assertTrue(localFs.mkdirs(new Path(dir5)));
      assertTrue(localFs.mkdirs(new Path(dir6)));

      int inDir5=0, inDir6=0;
      for(int i = 0; i < TRIALS; ++i) {
        File result = createTempFile();
        if(result.getPath().startsWith(
              new Path(dir5, FILENAME).toUri().getPath())) {
          inDir5++;
        } else if(result.getPath().startsWith(
              new Path(dir6, FILENAME).toUri().getPath())) {
          inDir6++;
        }
        result.delete();
      }

      assertTrue(inDir5 + inDir6 == TRIALS);

    } finally {
      rmBufferDirs();
    }
  }

  /** Two buffer dirs. The first dir does not exist & is on a read-only disk;
   * The second dir exists & is RW
   * getLocalPathForWrite with checkAccess set to false should create a parent
   * directory. With checkAccess true, the directory should not be created.
   * @throws Exception
   */
  @Test (timeout = 30000)
  public void testLocalPathForWriteDirCreation() throws IOException {
    String dir0 = buildBufferDir(ROOT, 0);
    String dir1 = buildBufferDir(ROOT, 1);
    try {
      conf.set(CONTEXT, dir0 + "," + dir1);
      assertTrue(localFs.mkdirs(new Path(dir1)));
      BUFFER_ROOT.setReadOnly();
      Path p1 =
        dirAllocator.getLocalPathForWrite("p1/x", SMALL_FILE_SIZE, conf);
      assertTrue(localFs.getFileStatus(p1.getParent()).isDirectory());

      Path p2 =
        dirAllocator.getLocalPathForWrite("p2/x", SMALL_FILE_SIZE, conf,
            false);
      try {
        localFs.getFileStatus(p2.getParent());
      } catch (Exception e) {
        assertEquals(e.getClass(), FileNotFoundException.class);
      }
    } finally {
      Shell.execCommand(Shell.getSetPermissionCommand("u+w", false,
                                                      BUFFER_DIR_ROOT));
      rmBufferDirs();
    }
  }

  /*
   * Test when mapred.local.dir not configured and called
   * getLocalPathForWrite
   */
  @Test (timeout = 30000)
  public void testShouldNotthrowNPE() throws Exception {
    Configuration conf1 = new Configuration();
    try {
      dirAllocator.getLocalPathForWrite("/test", conf1);
      fail("Exception not thrown when " + CONTEXT + " is not set");
    } catch (IOException e) {
      assertEquals(CONTEXT + " not configured", e.getMessage());
    } catch (NullPointerException e) {
      fail("Lack of configuration should not have thrown an NPE.");
    }
  }

  /** Test no side effect files are left over. After creating a temp
   * temp file, remove both the temp file and its parent. Verify that
   * no files or directories are left over as can happen when File objects
   * are mistakenly created from fully qualified path strings.
   * @throws IOException
   */
  @Test (timeout = 30000)
  public void testNoSideEffects() throws IOException {
    assumeTrue(!isWindows);
    String dir = buildBufferDir(ROOT, 0);
    try {
      conf.set(CONTEXT, dir);
      File result = dirAllocator.createTmpFileForWrite(FILENAME, -1, conf);
      assertTrue(result.delete());
      assertTrue(result.getParentFile().delete());
      assertFalse(new File(dir).exists());
    } finally {
      Shell.execCommand(Shell.getSetPermissionCommand("u+w", false,
                                                      BUFFER_DIR_ROOT));
      rmBufferDirs();
    }
  }

  /**
   * Test getLocalPathToRead() returns correct filename and "file" schema.
   *
   * @throws IOException
   */
  @Test (timeout = 30000)
  public void testGetLocalPathToRead() throws IOException {
    assumeTrue(!isWindows);
    String dir = buildBufferDir(ROOT, 0);
    try {
      conf.set(CONTEXT, dir);
      assertTrue(localFs.mkdirs(new Path(dir)));
      File f1 = dirAllocator.createTmpFileForWrite(FILENAME, SMALL_FILE_SIZE,
          conf);
      Path p1 = dirAllocator.getLocalPathToRead(f1.getName(), conf);
      assertEquals(f1.getName(), p1.getName());
      assertEquals("file", p1.getFileSystem(conf).getUri().getScheme());
    } finally {
      Shell.execCommand(Shell.getSetPermissionCommand("u+w", false,
                                                      BUFFER_DIR_ROOT));
      rmBufferDirs();
    }
  }

  /**
   * Test that {@link LocalDirAllocator#getAllLocalPathsToRead(String, Configuration)} 
   * returns correct filenames and "file" schema.
   *
   * @throws IOException
   */
  @Test (timeout = 30000)
  public void testGetAllLocalPathsToRead() throws IOException {
    assumeTrue(!isWindows);
    
    String dir0 = buildBufferDir(ROOT, 0);
    String dir1 = buildBufferDir(ROOT, 1);
    try {
      conf.set(CONTEXT, dir0 + "," + dir1);
      assertTrue(localFs.mkdirs(new Path(dir0)));
      assertTrue(localFs.mkdirs(new Path(dir1)));
      
      localFs.create(new Path(dir0 + Path.SEPARATOR + FILENAME));
      localFs.create(new Path(dir1 + Path.SEPARATOR + FILENAME));

      // check both the paths are returned as paths to read:  
      final Iterable<Path> pathIterable = dirAllocator.getAllLocalPathsToRead(FILENAME, conf);
      int count = 0;
      for (final Path p: pathIterable) {
        count++;
        assertEquals(FILENAME, p.getName());
        assertEquals("file", p.getFileSystem(conf).getUri().getScheme());
      }
      assertEquals(2, count);

      // test #next() while no element to iterate any more: 
      try {
        Path p = pathIterable.iterator().next();
        assertFalse("NoSuchElementException must be thrown, but returned ["+p
            +"] instead.", true); // exception expected
      } catch (NoSuchElementException nsee) {
        // okay
      }
      
      // test modification not allowed:
      final Iterable<Path> pathIterable2 = dirAllocator.getAllLocalPathsToRead(FILENAME, conf);
      final Iterator<Path> it = pathIterable2.iterator();
      try {
        it.remove();
        assertFalse(true); // exception expected
      } catch (UnsupportedOperationException uoe) {
        // okay
      }
    } finally {
      Shell.execCommand(new String[] { "chmod", "u+w", BUFFER_DIR_ROOT });
      rmBufferDirs();
    }
  }
  
  @Test (timeout = 30000)
  public void testRemoveContext() throws IOException {
    String dir = buildBufferDir(ROOT, 0);
    try {
      String contextCfgItemName = "application_1340842292563_0004.app.cache.dirs";
      conf.set(contextCfgItemName, dir);
      LocalDirAllocator localDirAllocator = new LocalDirAllocator(
          contextCfgItemName);
      localDirAllocator.getLocalPathForWrite("p1/x", SMALL_FILE_SIZE, conf);
      assertTrue(LocalDirAllocator.isContextValid(contextCfgItemName));
      LocalDirAllocator.removeContext(contextCfgItemName);
      assertFalse(LocalDirAllocator.isContextValid(contextCfgItemName));
    } finally {
      rmBufferDirs();
    }
  }

}
