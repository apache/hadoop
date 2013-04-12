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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests the FileStatus API.
 */
public class TestListFilesInFileContext {
  {
    ((Log4JLogger)FileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;

  final private static Configuration conf = new Configuration();
  private static MiniDFSCluster cluster;
  private static FileContext fc;
  final private static Path TEST_DIR = new Path("/main_");
  final private static int FILE_LEN = 10;
  final private static Path FILE1 = new Path(TEST_DIR, "file1");
  final private static Path DIR1 = new Path(TEST_DIR, "dir1");
  final private static Path FILE2 = new Path(DIR1, "file2");
  final private static Path FILE3 = new Path(DIR1, "file3");

  @BeforeClass
  public static void testSetUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).build();
    fc = FileContext.getFileContext(cluster.getConfiguration(0));
    fc.delete(TEST_DIR, true);
  }
  
  private static void writeFile(FileContext fc, Path name, int fileSize)
  throws IOException {
    // Create and write a file that contains three blocks of data
    FSDataOutputStream stm = fc.create(name, EnumSet.of(CreateFlag.CREATE),
        Options.CreateOpts.createParent());
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  @AfterClass
  public static void testShutdown() throws Exception {
    cluster.shutdown();
  }

  /** Test when input path is a file */
  @Test
  public void testFile() throws IOException {
    fc.mkdir(TEST_DIR, FsPermission.getDefault(), true);
    writeFile(fc, FILE1, FILE_LEN);

    RemoteIterator<LocatedFileStatus> itor = fc.util().listFiles(
        FILE1, true);
    LocatedFileStatus stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fc.makeQualified(FILE1), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
    
    itor = fc.util().listFiles(FILE1, false);
    stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fc.makeQualified(FILE1), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
  }

  @After
  public void cleanDir() throws IOException {
    fc.delete(TEST_DIR, true);
  }

  /** Test when input path is a directory */
  @Test
  public void testDirectory() throws IOException {
    fc.mkdir(DIR1, FsPermission.getDefault(), true);

    // test empty directory
    RemoteIterator<LocatedFileStatus> itor = fc.util().listFiles(
        DIR1, true);
    assertFalse(itor.hasNext());
    itor = fc.util().listFiles(DIR1, false);
    assertFalse(itor.hasNext());
    
    // testing directory with 1 file
    writeFile(fc, FILE2, FILE_LEN);
    
    itor = fc.util().listFiles(DIR1, true);
    LocatedFileStatus stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fc.makeQualified(FILE2), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
    
    itor = fc.util().listFiles(DIR1, false);
    stat = itor.next();
    assertFalse(itor.hasNext());
    assertTrue(stat.isFile());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fc.makeQualified(FILE2), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);

    // test more complicated directory
    writeFile(fc, FILE1, FILE_LEN);
    writeFile(fc, FILE3, FILE_LEN);

    itor = fc.util().listFiles(TEST_DIR, true);
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fc.makeQualified(FILE2), stat.getPath());
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fc.makeQualified(FILE3), stat.getPath());
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fc.makeQualified(FILE1), stat.getPath());
    assertFalse(itor.hasNext());
    
    itor = fc.util().listFiles(TEST_DIR, false);
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fc.makeQualified(FILE1), stat.getPath());
    assertFalse(itor.hasNext());
  }

  /** Test when input patch has a symbolic links as its children */
  @Test
  public void testSymbolicLinks() throws IOException {
    writeFile(fc, FILE1, FILE_LEN);
    writeFile(fc, FILE2, FILE_LEN);
    writeFile(fc, FILE3, FILE_LEN);
    
    Path dir4 = new Path(TEST_DIR, "dir4");
    Path dir5 = new Path(dir4, "dir5");
    Path file4 = new Path(dir4, "file4");
    
    fc.createSymlink(DIR1, dir5, true);
    fc.createSymlink(FILE1, file4, true);
    
    RemoteIterator<LocatedFileStatus> itor = fc.util().listFiles(dir4, true);
    LocatedFileStatus stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fc.makeQualified(FILE2), stat.getPath());
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fc.makeQualified(FILE3), stat.getPath());
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fc.makeQualified(FILE1), stat.getPath());
    assertFalse(itor.hasNext());
    
    itor = fc.util().listFiles(dir4, false);
    stat = itor.next();
    assertTrue(stat.isFile());
    assertEquals(fc.makeQualified(FILE1), stat.getPath());
    assertFalse(itor.hasNext());
  }
}
