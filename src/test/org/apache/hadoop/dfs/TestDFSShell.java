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
package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;


/**
 * This class tests commands from DFSShell.
 */
public class TestDFSShell extends TestCase {
  private static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');
  
  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    DataOutputStream stm = fileSys.create(name);
    stm.writeBytes("dhruba: " + name);
    stm.close();
  }

  public void testCopyToLocal() throws IOException {
    Configuration conf = new Configuration();
    /* This tests some properties of ChecksumFileSystem as well.
     * Make sure that we create ChecksumDFS */
    conf.set("fs.hdfs.impl",
             "org.apache.hadoop.dfs.ChecksumDistributedFileSystem");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof ChecksumDistributedFileSystem);
    ChecksumDistributedFileSystem dfs = (ChecksumDistributedFileSystem)fs;
    FsShell shell = new FsShell();
    shell.setConf(conf);

    try {
      {
        // create a tree
        //   ROOT
        //   |- f1
        //   |- f2
        //   + sub
        //      |- f3
        //      |- f4
        Path root = new Path("/test/copyToLocal");
        assertTrue(dfs.mkdirs(root));
        assertTrue(dfs.exists(root));
        assertTrue(dfs.isDirectory(root));

        Path sub = new Path(root, "sub");
        assertTrue(dfs.mkdirs(sub));
        assertTrue(dfs.exists(sub));
        assertTrue(dfs.isDirectory(sub));

        Path f1 = new Path(root, "f1");
        writeFile(dfs, f1);
        assertTrue(dfs.exists(f1));

        Path f2 = new Path(root, "f2");
        writeFile(dfs, f2);
        assertTrue(dfs.exists(f2));

        Path f3 = new Path(sub, "f3");
        writeFile(dfs, f3);
        assertTrue(dfs.exists(f3));

        Path f4 = new Path(sub, "f4");
        writeFile(dfs, f4);
        assertTrue(dfs.exists(f4));
      }


      // Verify copying the tree
      {
        String[] args = {"-copyToLocal", "/test/copyToLocal", TEST_ROOT_DIR};
        try {
          assertEquals(0, shell.run(args));
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }

        File f1 = new File(TEST_ROOT_DIR, "f1");
        assertTrue("Copying failed.", f1.isFile());

        File f2 = new File(TEST_ROOT_DIR, "f2");
        assertTrue("Copying failed.", f2.isFile());

        File sub = new File(TEST_ROOT_DIR, "sub");
        assertTrue("Copying failed.", sub.isDirectory());

        File f3 = new File(sub, "f3");
        assertTrue("Copying failed.", f3.exists());

        File f4 = new File(sub, "f4");
        assertTrue("Copying failed.", f4.exists());

        f1.delete();
        f2.delete();
        f3.delete();
        f4.delete();
        sub.delete();
      }
    } finally {
      try {
        dfs.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }

  /**
   * Tests various options of DFSShell.
   */
  public void testDFSShell() throws IOException {
    Configuration conf = new Configuration();
    /* This tests some properties of ChecksumFileSystem as well.
     * Make sure that we create ChecksumDFS */
    conf.set("fs.hdfs.impl",
             "org.apache.hadoop.dfs.ChecksumDistributedFileSystem");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
            fs instanceof ChecksumDistributedFileSystem);
    ChecksumDistributedFileSystem fileSys = (ChecksumDistributedFileSystem)fs;
    FsShell shell = new FsShell();
    shell.setConf(conf);

    try {
      // First create a new directory with mkdirs
      Path myPath = new Path("/test/mkdirs");
      assertTrue(fileSys.mkdirs(myPath));
      assertTrue(fileSys.exists(myPath));
      assertTrue(fileSys.mkdirs(myPath));

      // Second, create a file in that directory.
      Path myFile = new Path("/test/mkdirs/myFile");
      writeFile(fileSys, myFile);
      assertTrue(fileSys.exists(myFile));

      // Verify that we can read the file
      {
        String[] args = new String[2];
        args[0] = "-cat";
        args[1] = "/test/mkdirs/myFile";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run: " +
                             StringUtils.stringifyException(e)); 
        }
        assertTrue(val == 0);
      }

      // Verify that we can get with and without crc
      {
        File testFile = new File(TEST_ROOT_DIR, "myFile");
        File checksumFile = new File(fileSys.getChecksumFile(
                                                             new Path(testFile.getAbsolutePath())).toString());
        testFile.delete();
        checksumFile.delete();
          
        String[] args = new String[3];
        args[0] = "-get";
        args[1] = "/test/mkdirs";
        args[2] = TEST_ROOT_DIR;
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
        assertTrue("Copying failed.", testFile.exists());
        assertTrue("Checksum file " + checksumFile+" is copied.", !checksumFile.exists());
        testFile.delete();
      }
      {
        File testFile = new File(TEST_ROOT_DIR, "myFile");
        File checksumFile = new File(fileSys.getChecksumFile(
                                                             new Path(testFile.getAbsolutePath())).toString());
        testFile.delete();
        checksumFile.delete();
          
        String[] args = new String[4];
        args[0] = "-get";
        args[1] = "-crc";
        args[2] = "/test/mkdirs";
        args[3] = TEST_ROOT_DIR;
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
          
        assertTrue("Copying data file failed.", testFile.exists());
        assertTrue("Checksum file " + checksumFile+" not copied.", checksumFile.exists());
        testFile.delete();
        checksumFile.delete();
      }
      // Verify that we get an error while trying to read an nonexistent file
      {
        String[] args = new String[2];
        args[0] = "-cat";
        args[1] = "/test/mkdirs/myFile1";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val != 0);
      }

      // Verify that we get an error while trying to delete an nonexistent file
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile1";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val != 0);
      }

      // Verify that we succeed in removing the file we created
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
      }

      // Verify touch/test
      {
        String[] args = new String[2];
        args[0] = "-touchz";
        args[1] = "/test/mkdirs/noFileHere";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == 0);

        args[0] = "-test";
        args[1] = "-e " + args[1];
        try {
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == 0);
      }
        
    } finally {
      try {
        fileSys.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }
}
