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

import junit.framework.TestCase;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;

/**
 * This class tests the FileStatus API.
 */
public class TestFileStatus extends TestCase {
  {
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;

  private static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');
  
  private void writeFile(FileSystem fileSys, Path name, int repl,
                         int fileSize, int blockSize)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }

  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    DFSTestUtil.waitReplication(fileSys, name, (short) repl);
  }


  /**
   * Tests various options of DFSShell.
   */
  public void testFileStatus() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    final HftpFileSystem hftpfs = cluster.getHftpFileSystem();
    final DFSClient dfsClient = new DFSClient(NameNode.getAddress(conf), conf);
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      System.out.println("Path : \"" + path.toString() + "\"");
      System.out.println(fs.isDirectory(path));
      System.out.println(fs.getFileStatus(path).isDir()); 
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDir() == true);
      
      // make sure getFileInfo returns null for files which do not exist
      HdfsFileStatus fileInfo = dfsClient.getFileInfo("/noSuchFile");
      assertTrue(fileInfo == null);

      // create a file in home directory
      //
      Path file1 = new Path("filestatus.dat");
      writeFile(fs, file1, 1, fileSize, blockSize);
      System.out.println("Created file filestatus.dat with one "
                         + " replicas.");
      checkFile(fs, file1, 1);
      System.out.println("Path : \"" + file1 + "\"");

      // test getFileStatus on a file
      FileStatus status = fs.getFileStatus(file1);
      assertTrue(file1 + " should be a file", 
          status.isDir() == false);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      assertTrue(status.getLen() == fileSize);
      assertEquals(fs.makeQualified(file1).toString(), 
          status.getPath().toString());

      // test listStatus on a file
      FileStatus[] stats = fs.listStatus(file1);
      assertEquals(1, stats.length);
      status = stats[0];
      assertTrue(file1 + " should be a file", 
          status.isDir() == false);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      assertTrue(status.getLen() == fileSize);
      assertEquals(fs.makeQualified(file1).toString(), 
          status.getPath().toString());

      // create an empty directory
      //
      Path parentDir = new Path("/test");
      Path dir = new Path("/test/mkdirs");
      assertTrue(fs.mkdirs(dir));
      assertTrue(fs.exists(dir));
      System.out.println("Dir : \"" + dir + "\"");

      // test getFileStatus on an empty directory
      status = fs.getFileStatus(dir);
      assertTrue(dir + " should be a directory", status.isDir());
      assertTrue(dir + " should be zero size ", status.getLen() == 0);
      assertEquals(fs.makeQualified(dir).toString(), 
          status.getPath().toString());

      // test listStatus on an empty directory
      stats = fs.listStatus(dir);
      assertEquals(dir + " should be empty", 0, stats.length);
      assertEquals(dir + " should be zero size ",
          0, fs.getContentSummary(dir).getLength());
      assertEquals(dir + " should be zero size using hftp",
          0, hftpfs.getContentSummary(dir).getLength());
      assertTrue(dir + " should be zero size ",
                 fs.getFileStatus(dir).getLen() == 0);
      System.out.println("Dir : \"" + dir + "\"");

      // create another file that is smaller than a block.
      //
      Path file2 = new Path(dir, "filestatus2.dat");
      writeFile(fs, file2, 1, blockSize/4, blockSize);
      System.out.println("Created file filestatus2.dat with one "
                         + " replicas.");
      checkFile(fs, file2, 1);
      System.out.println("Path : \"" + file2 + "\"");

      // verify file attributes
      status = fs.getFileStatus(file2);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      assertEquals(fs.makeQualified(file2).toString(), 
          status.getPath().toString());

      // create another file in the same directory
      Path file3 = new Path(dir, "filestatus3.dat");
      writeFile(fs, file3, 1, blockSize/4, blockSize);
      System.out.println("Created file filestatus3.dat with one "
                         + " replicas.");
      checkFile(fs, file3, 1);

      // verify that the size of the directory increased by the size 
      // of the two files
      final int expected = blockSize/2;  
      assertEquals(dir + " size should be " + expected, 
          expected, fs.getContentSummary(dir).getLength());
      assertEquals(dir + " size should be " + expected + " using hftp", 
          expected, hftpfs.getContentSummary(dir).getLength());
       
       // test listStatus on a non-empty directory
       stats = fs.listStatus(dir);
       assertEquals(dir + " should have two entries", 2, stats.length);
       String qualifiedFile2 = fs.makeQualified(file2).toString();
       String qualifiedFile3 = fs.makeQualified(file3).toString();
       for(FileStatus stat:stats) {
         String statusFullName = stat.getPath().toString();
         assertTrue(qualifiedFile2.equals(statusFullName)
           || qualifiedFile3.toString().equals(statusFullName));
       }    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
