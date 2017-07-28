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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.BeforeClass;
import org.slf4j.event.Level;

/**
 * This class tests the FileStatus API.
 */
public class TestListFiles {
  static {
    GenericTestUtils.setLogLevel(FileSystem.LOG, Level.TRACE);
  }

  static final long seed = 0xDEADBEEFL;

  final protected static Configuration conf = new Configuration();
  protected static FileSystem fs;
  protected static Path TEST_DIR;
  final private static int FILE_LEN = 10;
  private static Path FILE1;
  private static Path DIR1;
  private static Path FILE2;
  private static Path FILE3;

  static {
    setTestPaths(new Path(GenericTestUtils.getTempPath("testlistfiles"),
        "main_"));
  }

  protected static Path getTestDir() {
    return TEST_DIR;
  }

  /**
   * Sets the root testing directory and reinitializes any additional test paths
   * that are under the root.  This method is intended to be called from a
   * subclass's @BeforeClass method if there is a need to override the testing
   * directory.
   * 
   * @param testDir Path root testing directory
   */
  protected static void setTestPaths(Path testDir) {
    TEST_DIR = testDir;
    FILE1 = new Path(TEST_DIR, "file1");
    DIR1 = new Path(TEST_DIR, "dir1");
    FILE2 = new Path(DIR1, "file2");
    FILE3 = new Path(DIR1, "file3");
  }

  @BeforeClass
  public static void testSetUp() throws Exception {
    fs = FileSystem.getLocal(conf);
    fs.delete(TEST_DIR, true);
  }
  
  private static void writeFile(FileSystem fileSys, Path name, int fileSize)
  throws IOException {
    // Create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  /** Test when input path is a file */
  @Test
  public void testFile() throws IOException {
    fs.mkdirs(TEST_DIR);
    writeFile(fs, FILE1, FILE_LEN);

    RemoteIterator<LocatedFileStatus> itor = fs.listFiles(
        FILE1, true);
    LocatedFileStatus stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fs.makeQualified(FILE1), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
    
    itor = fs.listFiles(FILE1, false);
    stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fs.makeQualified(FILE1), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
    
    fs.delete(FILE1, true);
  }


  /** Test when input path is a directory */
  @Test
  public void testDirectory() throws IOException {
    fs.mkdirs(DIR1);

    // test empty directory
    RemoteIterator<LocatedFileStatus> itor = fs.listFiles(
        DIR1, true);
    assertFalse(itor.hasNext());
    itor = fs.listFiles(DIR1, false);
    assertFalse(itor.hasNext());
    
    // testing directory with 1 file
    writeFile(fs, FILE2, FILE_LEN);    
    itor = fs.listFiles(DIR1, true);
    LocatedFileStatus stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fs.makeQualified(FILE2), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
    
    itor = fs.listFiles(DIR1, false);
    stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fs.makeQualified(FILE2), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);

    // test more complicated directory
    writeFile(fs, FILE1, FILE_LEN);
    writeFile(fs, FILE3, FILE_LEN);

    Set<Path> filesToFind = new HashSet<Path>();
    filesToFind.add(fs.makeQualified(FILE1));
    filesToFind.add(fs.makeQualified(FILE2));
    filesToFind.add(fs.makeQualified(FILE3));

    itor = fs.listFiles(TEST_DIR, true);
    stat = itor.next();
    assertTrue(stat.isFile());
    assertTrue("Path " + stat.getPath() + " unexpected",
      filesToFind.remove(stat.getPath()));

    stat = itor.next();
    assertTrue(stat.isFile());
    assertTrue("Path " + stat.getPath() + " unexpected",
      filesToFind.remove(stat.getPath()));

    stat = itor.next();
    assertTrue(stat.isFile());
    assertTrue("Path " + stat.getPath() + " unexpected",
      filesToFind.remove(stat.getPath()));
    assertFalse(itor.hasNext());
    assertTrue(filesToFind.isEmpty());
    
    itor = fs.listFiles(TEST_DIR, false);
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fs.makeQualified(FILE1), stat.getPath());
    assertFalse(itor.hasNext());
    
    fs.delete(TEST_DIR, true);
  }
}
