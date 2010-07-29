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
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.BeforeClass;

/**
 * This class tests the FileStatus API.
 */
public class TestListFiles {
  {
    ((Log4JLogger)FileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;

  final protected static Configuration conf = new Configuration();
  protected static FileSystem fs;
  final protected static Path TEST_DIR = getTestDir();
  final private static int FILE_LEN = 10;
  final private static Path FILE1 = new Path(TEST_DIR, "file1");
  final private static Path DIR1 = new Path(TEST_DIR, "dir1");
  final private static Path FILE2 = new Path(DIR1, "file2");
  final private static Path FILE3 = new Path(DIR1, "file3");

  protected static Path getTestDir() {
    return new Path(
      System.getProperty("test.build.data","build/test/data/work-dir/localfs"),
      "main_");
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

    Iterator<LocatedFileStatus> itor = fs.listFiles(
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

    Iterator<LocatedFileStatus> itor = fs.listFiles(
        DIR1, true);
    assertFalse(itor.hasNext());
    itor = fs.listFiles(DIR1, false);
    assertFalse(itor.hasNext());
    
    writeFile(fs, FILE2, FILE_LEN);
    
    // test empty directory
    itor = fs.listFiles(DIR1, true);
    LocatedFileStatus stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fs.makeQualified(FILE2), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
    
    // testing directory with 1 file
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

    itor = fs.listFiles(TEST_DIR, true);
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fs.makeQualified(FILE1), stat.getPath());
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fs.makeQualified(FILE2), stat.getPath());
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fs.makeQualified(FILE3), stat.getPath());
    assertFalse(itor.hasNext());
    
    itor = fs.listFiles(TEST_DIR, false);
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fs.makeQualified(FILE1), stat.getPath());
    assertFalse(itor.hasNext());
    
    fs.delete(TEST_DIR, true);
  }
}
