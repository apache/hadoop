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
import java.security.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.StringUtils;

/**
 * This class tests commands from DFSShell.
 */
public class TestDFSShell extends TestCase {
  static final String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');

  static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    out.writeBytes("dhruba: " + f);
    out.close();
    assertTrue(fs.exists(f));
    return f;
  }

  static Path mkdir(FileSystem fs, Path p) throws IOException {
    assertTrue(fs.mkdirs(p));
    assertTrue(fs.exists(p));
    assertTrue(fs.getFileStatus(p).isDir());
    return p;
  }

  static File createLocalFile(File f) throws IOException {
    assertTrue(!f.exists());
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(f)));
    out.println(f.getAbsolutePath());
    out.close();
    assertTrue(f.exists());
    assertTrue(f.isFile());
    return f;
  }

  static void show(String s) {
    System.out.println(Thread.currentThread().getStackTrace()[2] + " " + s);
  }

  public void testZeroSizeFile() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;

    try {
      //create a zero size file
      final File f1 = new File(TEST_ROOT_DIR, "f1");
      assertTrue(!f1.exists());
      assertTrue(f1.createNewFile());
      assertTrue(f1.exists());
      assertTrue(f1.isFile());
      assertEquals(0L, f1.length());
      
      //copy to remote
      final Path root = mkdir(dfs, new Path("/test/zeroSizeFile"));
      final Path remotef = new Path(root, "dst");
      show("copy local " + f1 + " to remote " + remotef);
      dfs.copyFromLocalFile(false, false, new Path(f1.getPath()), remotef);
      
      //getBlockSize() should not throw exception
      show("Block size = " + dfs.getFileStatus(remotef).getBlockSize());

      //copy back
      final File f2 = new File(TEST_ROOT_DIR, "f2");
      assertTrue(!f2.exists());
      dfs.copyToLocalFile(remotef, new Path(f2.getPath()));
      assertTrue(f2.exists());
      assertTrue(f2.isFile());
      assertEquals(0L, f2.length());
  
      f1.delete();
      f2.delete();
    } finally {
      try {dfs.close();} catch (Exception e) {}
      cluster.shutdown();
    }
  }

  public void testPut() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;

    try {
      final File f1 = createLocalFile(new File(TEST_ROOT_DIR, "f1"));
      final File f2 = createLocalFile(new File(TEST_ROOT_DIR, "f2"));
  
      final Path root = mkdir(dfs, new Path("/test/put"));
      final Path dst = new Path(root, "dst");
  
      show("begin");
      
      final Thread copy2ndFileThread = new Thread() {
        public void run() {
          try {
            show("copy local " + f2 + " to remote " + dst);
            dfs.copyFromLocalFile(false, false, new Path(f2.getPath()), dst);
          } catch (IOException ioe) {
            show("good " + StringUtils.stringifyException(ioe));
            return;
          }
          //should not be here, must got IOException
          assertTrue(false);
        }
      };
      
      //use SecurityManager to pause the copying of f1 and begin copying f2
      SecurityManager sm = System.getSecurityManager();
      System.out.println("SecurityManager = " + sm);
      System.setSecurityManager(new SecurityManager() {
        private boolean firstTime = true;
  
        public void checkPermission(Permission perm) {
          if (firstTime) {
            Thread t = Thread.currentThread();
            if (!t.toString().contains("DataNode")) {
              String s = "" + Arrays.asList(t.getStackTrace());
              if (s.contains("FileUtil.copyContent")) {
                //pause at FileUtil.copyContent
  
                firstTime = false;
                copy2ndFileThread.start();
                try {Thread.sleep(5000);} catch (InterruptedException e) {}
              }
            }
          }
        }
      });
      show("copy local " + f1 + " to remote " + dst);
      dfs.copyFromLocalFile(false, false, new Path(f1.getPath()), dst);
      show("done");
  
      try {copy2ndFileThread.join();} catch (InterruptedException e) { }
      System.setSecurityManager(sm);
      f1.delete();
      f2.delete();
    } finally {
      try {dfs.close();} catch (Exception e) {}
      cluster.shutdown();
    }
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
        //   ROOT2
        //   |- f1
        Path root = mkdir(dfs, new Path("/test/copyToLocal"));
        Path sub = mkdir(dfs, new Path(root, "sub"));
        Path root2 = mkdir(dfs, new Path("/test/copyToLocal2"));        

        writeFile(fs, new Path(root, "f1"));
        writeFile(fs, new Path(root, "f2"));
        writeFile(fs, new Path(sub, "f3"));
        writeFile(fs, new Path(sub, "f4"));
        writeFile(fs, new Path(root2, "f1"));        
      }

      // Verify copying the tree
      {
        String[] args = {"-copyToLocal", "/test/copyToLocal*", TEST_ROOT_DIR};
        try {
          assertEquals(0, shell.run(args));
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }

        File localroot = new File(TEST_ROOT_DIR, "copyToLocal");
        File localroot2 = new File(TEST_ROOT_DIR, "copyToLocal2");        
        
        File f1 = new File(localroot, "f1");
        assertTrue("Copying failed.", f1.isFile());

        File f2 = new File(localroot, "f2");
        assertTrue("Copying failed.", f2.isFile());

        File sub = new File(localroot, "sub");
        assertTrue("Copying failed.", sub.isDirectory());

        File f3 = new File(sub, "f3");
        assertTrue("Copying failed.", f3.isFile());

        File f4 = new File(sub, "f4");
        assertTrue("Copying failed.", f4.isFile());
        
        File f5 = new File(localroot2, "f1");
        assertTrue("Copying failed.", f5.isFile());        

        f1.delete();
        f2.delete();
        f3.delete();
        f4.delete();
        f5.delete();
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
        File testFile = new File(TEST_ROOT_DIR, "mkdirs/myFile");
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
        File testFile = new File(TEST_ROOT_DIR, "mkdirs/myFile");
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

      // Verify that cp from a directory to a subdirectory fails
      {
        String[] args = new String[2];
        args[0] = "-mkdir";
        args[1] = "/test/dir1";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == 0);

        // this should fail
        String[] args1 = new String[3];
        args1[0] = "-cp";
        args1[1] = "/test/dir1";
        args1[2] = "/test/dir1/dir2";
        val = 0;
        try {
          val = shell.run(args1);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == -1);

        // this should succeed
        args1[0] = "-cp";
        args1[1] = "/test/dir1";
        args1[2] = "/test/dir1foo";
        val = -1;
        try {
          val = shell.run(args1);
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
