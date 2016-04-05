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

import java.io.*;
import java.security.Permission;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;
import static org.hamcrest.core.StringContains.containsString;

import com.google.common.collect.Lists;

/**
 * This class tests commands from DFSShell.
 */
public class TestDFSShell {
  private static final Log LOG = LogFactory.getLog(TestDFSShell.class);
  private static final AtomicInteger counter = new AtomicInteger();
  private final int SUCCESS = 0;
  private final int ERROR = 1;

  static final String TEST_ROOT_DIR = PathUtils.getTestDirName(TestDFSShell.class);

  private static final String RAW_A1 = "raw.a1";
  private static final String TRUSTED_A1 = "trusted.a1";
  private static final String USER_A1 = "user.a1";
  private static final byte[] RAW_A1_VALUE = new byte[]{0x32, 0x32, 0x32};
  private static final byte[] TRUSTED_A1_VALUE = new byte[]{0x31, 0x31, 0x31};
  private static final byte[] USER_A1_VALUE = new byte[]{0x31, 0x32, 0x33};

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
    assertTrue(fs.getFileStatus(p).isDirectory());
    return p;
  }

  static File createLocalFile(File f) throws IOException {
    assertTrue(!f.exists());
    PrintWriter out = new PrintWriter(f);
    out.print("createLocalFile: " + f.getAbsolutePath());
    out.flush();
    out.close();
    assertTrue(f.exists());
    assertTrue(f.isFile());
    return f;
  }

  static File createLocalFileWithRandomData(int fileLength, File f)
      throws IOException {
    assertTrue(!f.exists());
    f.createNewFile();
    FileOutputStream out = new FileOutputStream(f.toString());
    byte[] buffer = new byte[fileLength];
    out.write(buffer);
    out.flush();
    out.close();
    return f;
  }

  static void show(String s) {
    System.out.println(Thread.currentThread().getStackTrace()[2] + " " + s);
  }

  @Test (timeout = 30000)
  public void testZeroSizeFile() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
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
  
  @Test (timeout = 30000)
  public void testRecursiveRm() throws IOException {
	  Configuration conf = new HdfsConfiguration();
	  MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
	  FileSystem fs = cluster.getFileSystem();
	  assertTrue("Not a HDFS: " + fs.getUri(), 
			  fs instanceof DistributedFileSystem);
	  try {
      fs.mkdirs(new Path(new Path("parent"), "child"));
      try {
        fs.delete(new Path("parent"), false);
        assert(false); // should never reach here.
      } catch(IOException e) {
         //should have thrown an exception
      }
      try {
        fs.delete(new Path("parent"), true);
      } catch(IOException e) {
        assert(false);
      }
    } finally {  
      try { fs.close();}catch(IOException e){};
      cluster.shutdown();
    }
  }
    
  @Test (timeout = 30000)
  public void testDu() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    PrintStream psBackup = System.out;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream psOut = new PrintStream(out);
    System.setOut(psOut);
    FsShell shell = new FsShell();
    shell.setConf(conf);
    
    try {
      Path myPath = new Path("/test/dir");
      assertTrue(fs.mkdirs(myPath));
      assertTrue(fs.exists(myPath));
      Path myFile = new Path("/test/dir/file");
      writeFile(fs, myFile);
      assertTrue(fs.exists(myFile));
      Path myFile2 = new Path("/test/dir/file2");
      writeFile(fs, myFile2);
      assertTrue(fs.exists(myFile2));
      Long myFileLength = fs.getFileStatus(myFile).getLen();
      Long myFile2Length = fs.getFileStatus(myFile2).getLen();
      
      String[] args = new String[2];
      args[0] = "-du";
      args[1] = "/test/dir";
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
      }
      assertTrue(val == 0);
      String returnString = out.toString();
      out.reset();
      // Check if size matchs as expected
      assertThat(returnString, containsString(myFileLength.toString()));
      assertThat(returnString, containsString(myFile2Length.toString()));
      
      // Check that -du -s reports the state of the snapshot
      String snapshotName = "ss1";
      Path snapshotPath = new Path(myPath, ".snapshot/" + snapshotName);
      fs.allowSnapshot(myPath);
      assertThat(fs.createSnapshot(myPath, snapshotName), is(snapshotPath));
      assertThat(fs.delete(myFile, false), is(true));
      assertThat(fs.exists(myFile), is(false));

      args = new String[3];
      args[0] = "-du";
      args[1] = "-s";
      args[2] = snapshotPath.toString();
      val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertThat(val, is(0));
      returnString = out.toString();
      out.reset();
      Long combinedLength = myFileLength + myFile2Length;
      assertThat(returnString, containsString(combinedLength.toString()));
    } finally {
      System.setOut(psBackup);
      cluster.shutdown();
    }
                                  
  }

  @Test (timeout = 30000)
  public void testPut() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;

    try {
      // remove left over crc files:
      new File(TEST_ROOT_DIR, ".f1.crc").delete();
      new File(TEST_ROOT_DIR, ".f2.crc").delete();    
      final File f1 = createLocalFile(new File(TEST_ROOT_DIR, "f1"));
      final File f2 = createLocalFile(new File(TEST_ROOT_DIR, "f2"));
  
      final Path root = mkdir(dfs, new Path("/test/put"));
      final Path dst = new Path(root, "dst");
  
      show("begin");
      
      final Thread copy2ndFileThread = new Thread() {
        @Override
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
  
        @Override
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

      // copy multiple files to destination directory
      final Path destmultiple = mkdir(dfs, new Path("/test/putmultiple"));
      Path[] srcs = new Path[2];
      srcs[0] = new Path(f1.getPath());
      srcs[1] = new Path(f2.getPath());
      dfs.copyFromLocalFile(false, false, srcs, destmultiple);
      srcs[0] = new Path(destmultiple,"f1"); 
      srcs[1] = new Path(destmultiple,"f2"); 
      assertTrue(dfs.exists(srcs[0]));
      assertTrue(dfs.exists(srcs[1]));

      // move multiple files to destination directory
      final Path destmultiple2 = mkdir(dfs, new Path("/test/movemultiple"));
      srcs[0] = new Path(f1.getPath());
      srcs[1] = new Path(f2.getPath());
      dfs.moveFromLocalFile(srcs, destmultiple2);
      assertFalse(f1.exists());
      assertFalse(f2.exists());
      srcs[0] = new Path(destmultiple2, "f1");
      srcs[1] = new Path(destmultiple2, "f2");
      assertTrue(dfs.exists(srcs[0]));
      assertTrue(dfs.exists(srcs[1]));

      f1.delete();
      f2.delete();
    } finally {
      try {dfs.close();} catch (Exception e) {}
      cluster.shutdown();
    }
  }


  /** check command error outputs and exit statuses. */
  @Test (timeout = 30000)
  public void testErrOutPut() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    PrintStream bak = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem srcFs = cluster.getFileSystem();
      Path root = new Path("/nonexistentfile");
      bak = System.err;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      PrintStream tmp = new PrintStream(out);
      System.setErr(tmp);
      String[] argv = new String[2];
      argv[0] = "-cat";
      argv[1] = root.toUri().getPath();
      int ret = ToolRunner.run(new FsShell(), argv);
      assertEquals(" -cat returned 1 ", 1, ret);
      String returned = out.toString();
      assertTrue("cat does not print exceptions ",
          (returned.lastIndexOf("Exception") == -1));
      out.reset();
      argv[0] = "-rm";
      argv[1] = root.toString();
      FsShell shell = new FsShell();
      shell.setConf(conf);
      ret = ToolRunner.run(shell, argv);
      assertEquals(" -rm returned 1 ", 1, ret);
      returned = out.toString();
      out.reset();
      assertTrue("rm prints reasonable error ",
          (returned.lastIndexOf("No such file or directory") != -1));
      argv[0] = "-rmr";
      argv[1] = root.toString();
      ret = ToolRunner.run(shell, argv);
      assertEquals(" -rmr returned 1", 1, ret);
      returned = out.toString();
      assertTrue("rmr prints reasonable error ",
    		  (returned.lastIndexOf("No such file or directory") != -1));
      out.reset();
      argv[0] = "-du";
      argv[1] = "/nonexistentfile";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -du prints reasonable error ",
          (returned.lastIndexOf("No such file or directory") != -1));
      out.reset();
      argv[0] = "-dus";
      argv[1] = "/nonexistentfile";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -dus prints reasonable error",
          (returned.lastIndexOf("No such file or directory") != -1));
      out.reset();
      argv[0] = "-ls";
      argv[1] = "/nonexistenfile";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -ls does not return Found 0 items",
          (returned.lastIndexOf("Found 0") == -1));
      out.reset();
      argv[0] = "-ls";
      argv[1] = "/nonexistentfile";
      ret = ToolRunner.run(shell, argv);
      assertEquals(" -lsr should fail ", 1, ret);
      out.reset();
      srcFs.mkdirs(new Path("/testdir"));
      argv[0] = "-ls";
      argv[1] = "/testdir";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -ls does not print out anything ",
          (returned.lastIndexOf("Found 0") == -1));
      out.reset();
      argv[0] = "-ls";
      argv[1] = "/user/nonxistant/*";
      ret = ToolRunner.run(shell, argv);
      assertEquals(" -ls on nonexistent glob returns 1", 1, ret);
      out.reset();
      argv[0] = "-mkdir";
      argv[1] = "/testdir";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertEquals(" -mkdir returned 1 ", 1, ret);
      assertTrue(" -mkdir returned File exists", 
          (returned.lastIndexOf("File exists") != -1));
      Path testFile = new Path("/testfile");
      OutputStream outtmp = srcFs.create(testFile);
      outtmp.write(testFile.toString().getBytes());
      outtmp.close();
      out.reset();
      argv[0] = "-mkdir";
      argv[1] = "/testfile";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertEquals(" -mkdir returned 1", 1, ret);
      assertTrue(" -mkdir returned this is a file ",
          (returned.lastIndexOf("not a directory") != -1));
      out.reset();
      argv = new String[3];
      argv[0] = "-mv";
      argv[1] = "/testfile";
      argv[2] = "file";
      ret = ToolRunner.run(shell, argv);
      assertEquals("mv failed to rename", 1,  ret);
      out.reset();
      argv = new String[3];
      argv[0] = "-mv";
      argv[1] = "/testfile";
      argv[2] = "/testfiletest";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue("no output from rename", 
          (returned.lastIndexOf("Renamed") == -1));
      out.reset();
      argv[0] = "-mv";
      argv[1] = "/testfile";
      argv[2] = "/testfiletmp";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" unix like output",
          (returned.lastIndexOf("No such file or") != -1));
      out.reset();
      argv = new String[1];
      argv[0] = "-du";
      srcFs.mkdirs(srcFs.getHomeDirectory());
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertEquals(" no error ", 0, ret);
      assertTrue("empty path specified",
          (returned.lastIndexOf("empty string") == -1));
      out.reset();
      argv = new String[3];
      argv[0] = "-test";
      argv[1] = "-d";
      argv[2] = "/no/such/dir";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertEquals(" -test -d wrong result ", 1, ret);
      assertTrue(returned.isEmpty());
    } finally {
      if (bak != null) {
        System.setErr(bak);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testMoveWithTargetPortEmpty() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .format(true)
          .numDataNodes(2)
          .nameNodePort(8020)
          .waitSafeMode(true)
          .build();
      FileSystem srcFs = cluster.getFileSystem();
      FsShell shell = new FsShell();
      shell.setConf(conf);
      String[] argv = new String[2];
      argv[0] = "-mkdir";
      argv[1] = "/testfile";
      ToolRunner.run(shell, argv);
      argv = new String[3];
      argv[0] = "-mv";
      argv[1] = srcFs.getUri() + "/testfile";
      argv[2] = "hdfs://localhost/testfile2";
      int ret = ToolRunner.run(shell, argv);
      assertEquals("mv should have succeeded", 0, ret);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test (timeout = 30000)
  public void testURIPaths() throws Exception {
    Configuration srcConf = new HdfsConfiguration();
    Configuration dstConf = new HdfsConfiguration();
    MiniDFSCluster srcCluster =  null;
    MiniDFSCluster dstCluster = null;
    File bak = new File(PathUtils.getTestDir(getClass()), "dfs_tmp_uri");
    bak.mkdirs();
    try{
      srcCluster = new MiniDFSCluster.Builder(srcConf).numDataNodes(2).build();
      dstConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, bak.getAbsolutePath());
      dstCluster = new MiniDFSCluster.Builder(dstConf).numDataNodes(2).build();
      FileSystem srcFs = srcCluster.getFileSystem();
      FileSystem dstFs = dstCluster.getFileSystem();
      FsShell shell = new FsShell();
      shell.setConf(srcConf);
      //check for ls
      String[] argv = new String[2];
      argv[0] = "-ls";
      argv[1] = dstFs.getUri().toString() + "/";
      int ret = ToolRunner.run(shell, argv);
      assertEquals("ls works on remote uri ", 0, ret);
      //check for rm -r 
      dstFs.mkdirs(new Path("/hadoopdir"));
      argv = new String[2];
      argv[0] = "-rmr";
      argv[1] = dstFs.getUri().toString() + "/hadoopdir";
      ret = ToolRunner.run(shell, argv);
      assertEquals("-rmr works on remote uri " + argv[1], 0, ret);
      //check du 
      argv[0] = "-du";
      argv[1] = dstFs.getUri().toString() + "/";
      ret = ToolRunner.run(shell, argv);
      assertEquals("du works on remote uri ", 0, ret);
      //check put
      File furi = new File(TEST_ROOT_DIR, "furi");
      createLocalFile(furi);
      argv = new String[3];
      argv[0] = "-put";
      argv[1] = furi.toURI().toString();
      argv[2] = dstFs.getUri().toString() + "/furi";
      ret = ToolRunner.run(shell, argv);
      assertEquals(" put is working ", 0, ret);
      //check cp 
      argv[0] = "-cp";
      argv[1] = dstFs.getUri().toString() + "/furi";
      argv[2] = srcFs.getUri().toString() + "/furi";
      ret = ToolRunner.run(shell, argv);
      assertEquals(" cp is working ", 0, ret);
      assertTrue(srcFs.exists(new Path("/furi")));
      //check cat 
      argv = new String[2];
      argv[0] = "-cat";
      argv[1] = dstFs.getUri().toString() + "/furi";
      ret = ToolRunner.run(shell, argv);
      assertEquals(" cat is working ", 0, ret);
      //check chown
      dstFs.delete(new Path("/furi"), true);
      dstFs.delete(new Path("/hadoopdir"), true);
      String file = "/tmp/chownTest";
      Path path = new Path(file);
      Path parent = new Path("/tmp");
      Path root = new Path("/");
      TestDFSShell.writeFile(dstFs, path);
      runCmd(shell, "-chgrp", "-R", "herbivores", dstFs.getUri().toString() +"/*");
      confirmOwner(null, "herbivores", dstFs, parent, path);
      runCmd(shell, "-chown", "-R", ":reptiles", dstFs.getUri().toString() + "/");
      confirmOwner(null, "reptiles", dstFs, root, parent, path);
      //check if default hdfs:/// works 
      argv[0] = "-cat";
      argv[1] = "hdfs:///furi";
      ret = ToolRunner.run(shell, argv);
      assertEquals(" default works for cat", 0, ret);
      argv[0] = "-ls";
      argv[1] = "hdfs:///";
      ret = ToolRunner.run(shell, argv);
      assertEquals("default works for ls ", 0, ret);
      argv[0] = "-rmr";
      argv[1] = "hdfs:///furi";
      ret = ToolRunner.run(shell, argv);
      assertEquals("default works for rm/rmr", 0, ret);
    } finally {
      if (null != srcCluster) {
        srcCluster.shutdown();
      }
      if (null != dstCluster) {
        dstCluster.shutdown();
      }
    }
  }

  @Test (timeout = 30000)
  public void testText() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      final FileSystem dfs = cluster.getFileSystem();
      textTest(new Path("/texttest").makeQualified(dfs.getUri(),
            dfs.getWorkingDirectory()), conf);

      conf.set("fs.defaultFS", dfs.getUri().toString());
      final FileSystem lfs = FileSystem.getLocal(conf);
      textTest(new Path(TEST_ROOT_DIR, "texttest").makeQualified(lfs.getUri(),
            lfs.getWorkingDirectory()), conf);
    } finally {
      if (null != cluster) {
        cluster.shutdown();
      }
    }
  }

  private void textTest(Path root, Configuration conf) throws Exception {
    PrintStream bak = null;
    try {
      final FileSystem fs = root.getFileSystem(conf);
      fs.mkdirs(root);

      // Test the gzip type of files. Magic detection.
      OutputStream zout = new GZIPOutputStream(
          fs.create(new Path(root, "file.gz")));
      Random r = new Random();
      bak = System.out;
      ByteArrayOutputStream file = new ByteArrayOutputStream();
      for (int i = 0; i < 1024; ++i) {
        char c = Character.forDigit(r.nextInt(26) + 10, 36);
        file.write(c);
        zout.write(c);
      }
      zout.close();

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));

      String[] argv = new String[2];
      argv[0] = "-text";
      argv[1] = new Path(root, "file.gz").toString();
      int ret = ToolRunner.run(new FsShell(conf), argv);
      assertEquals("'-text " + argv[1] + " returned " + ret, 0, ret);
      assertTrue("Output doesn't match input",
          Arrays.equals(file.toByteArray(), out.toByteArray()));

      // Create a sequence file with a gz extension, to test proper
      // container detection. Magic detection.
      SequenceFile.Writer writer = SequenceFile.createWriter(
          conf,
          SequenceFile.Writer.file(new Path(root, "file.gz")),
          SequenceFile.Writer.keyClass(Text.class),
          SequenceFile.Writer.valueClass(Text.class));
      writer.append(new Text("Foo"), new Text("Bar"));
      writer.close();
      out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));
      argv = new String[2];
      argv[0] = "-text";
      argv[1] = new Path(root, "file.gz").toString();
      ret = ToolRunner.run(new FsShell(conf), argv);
      assertEquals("'-text " + argv[1] + " returned " + ret, 0, ret);
      assertTrue("Output doesn't match input",
          Arrays.equals("Foo\tBar\n".getBytes(), out.toByteArray()));
      out.reset();

      // Test deflate. Extension-based detection.
      OutputStream dout = new DeflaterOutputStream(
          fs.create(new Path(root, "file.deflate")));
      byte[] outbytes = "foo".getBytes();
      dout.write(outbytes);
      dout.close();
      out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));
      argv = new String[2];
      argv[0] = "-text";
      argv[1] = new Path(root, "file.deflate").toString();
      ret = ToolRunner.run(new FsShell(conf), argv);
      assertEquals("'-text " + argv[1] + " returned " + ret, 0, ret);
      assertTrue("Output doesn't match input",
          Arrays.equals(outbytes, out.toByteArray()));
      out.reset();

      // Test a simple codec. Extension based detection. We use
      // Bzip2 cause its non-native.
      CompressionCodec codec = ReflectionUtils.newInstance(BZip2Codec.class, conf);
      String extension = codec.getDefaultExtension();
      Path p = new Path(root, "file." + extension);
      OutputStream fout = new DataOutputStream(codec.createOutputStream(
          fs.create(p, true)));
      byte[] writebytes = "foo".getBytes();
      fout.write(writebytes);
      fout.close();
      out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));
      argv = new String[2];
      argv[0] = "-text";
      argv[1] = new Path(root, p).toString();
      ret = ToolRunner.run(new FsShell(conf), argv);
      assertEquals("'-text " + argv[1] + " returned " + ret, 0, ret);
      assertTrue("Output doesn't match input",
          Arrays.equals(writebytes, out.toByteArray()));
      out.reset();
    } finally {
      if (null != bak) {
        System.setOut(bak);
      }
    }
  }

  @Test (timeout = 30000)
  public void testCopyToLocal() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    DistributedFileSystem dfs = (DistributedFileSystem)fs;
    FsShell shell = new FsShell();
    shell.setConf(conf);

    try {
      String root = createTree(dfs, "copyToLocal");

      // Verify copying the tree
      {
        try {
          assertEquals(0,
              runCmd(shell, "-copyToLocal", root + "*", TEST_ROOT_DIR));
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
      // Verify copying non existing sources do not create zero byte
      // destination files
      {
        String[] args = {"-copyToLocal", "nosuchfile", TEST_ROOT_DIR};
        try {   
          assertEquals(1, shell.run(args));
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
        }                            
        File f6 = new File(TEST_ROOT_DIR, "nosuchfile");
        assertTrue(!f6.exists());
      }
    } finally {
      try {
        dfs.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }

  static String createTree(FileSystem fs, String name) throws IOException {
    // create a tree
    //   ROOT
    //   |- f1
    //   |- f2
    //   + sub
    //      |- f3
    //      |- f4
    //   ROOT2
    //   |- f1
    String path = "/test/" + name;
    Path root = mkdir(fs, new Path(path));
    Path sub = mkdir(fs, new Path(root, "sub"));
    Path root2 = mkdir(fs, new Path(path + "2"));        

    writeFile(fs, new Path(root, "f1"));
    writeFile(fs, new Path(root, "f2"));
    writeFile(fs, new Path(sub, "f3"));
    writeFile(fs, new Path(sub, "f4"));
    writeFile(fs, new Path(root2, "f1"));
    mkdir(fs, new Path(root2, "sub"));
    return path;
  }

  @Test (timeout = 30000)
  public void testCount() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    DistributedFileSystem dfs = cluster.getFileSystem();
    FsShell shell = new FsShell();
    shell.setConf(conf);

    try {
      String root = createTree(dfs, "count");

      // Verify the counts
      runCount(root, 2, 4, shell);
      runCount(root + "2", 2, 1, shell);
      runCount(root + "2/f1", 0, 1, shell);
      runCount(root + "2/sub", 1, 0, shell);

      final FileSystem localfs = FileSystem.getLocal(conf);
      Path localpath = new Path(TEST_ROOT_DIR, "testcount");
      localpath = localpath.makeQualified(localfs.getUri(),
          localfs.getWorkingDirectory());
      localfs.mkdirs(localpath);
      
      final String localstr = localpath.toString();
      System.out.println("localstr=" + localstr);
      runCount(localstr, 1, 0, shell);
      assertEquals(0, runCmd(shell, "-count", root, localstr));
    } finally {
      try {
        dfs.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }
  private static void runCount(String path, long dirs, long files, FsShell shell
    ) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(); 
    PrintStream out = new PrintStream(bytes);
    PrintStream oldOut = System.out;
    System.setOut(out);
    Scanner in = null;
    String results = null;
    try {
      runCmd(shell, "-count", path);
      results = bytes.toString();
      in = new Scanner(results);
      assertEquals(dirs, in.nextLong());
      assertEquals(files, in.nextLong());
    } finally {
      if (in!=null) in.close();
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.out.println("results:\n" + results);
    }
  }

  //throws IOException instead of Exception as shell.run() does.
  private static int runCmd(FsShell shell, String... args) throws IOException {
    StringBuilder cmdline = new StringBuilder("RUN:");
    for (String arg : args) cmdline.append(" " + arg);
    LOG.info(cmdline.toString());
    try {
      int exitCode;
      exitCode = shell.run(args);
      LOG.info("RUN: "+args[0]+" exit=" + exitCode);
      return exitCode;
    } catch (IOException e) {
      LOG.error("RUN: "+args[0]+" IOException="+e.getMessage());
      throw e;
    } catch (RuntimeException e) {
      LOG.error("RUN: "+args[0]+" RuntimeException="+e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error("RUN: "+args[0]+" Exception="+e.getMessage());
      throw new IOException(StringUtils.stringifyException(e));
    }
  }
  
  /**
   * Test chmod.
   */
  void testChmod(Configuration conf, FileSystem fs, String chmodDir) 
                                                    throws IOException {
    FsShell shell = new FsShell();
    shell.setConf(conf);
    
    try {
      //first make dir
      Path dir = new Path(chmodDir);
      fs.delete(dir, true);
      fs.mkdirs(dir);

      confirmPermissionChange(/* Setting */ "u+rwx,g=rw,o-rwx",
                             /* Should give */ "rwxrw----", fs, shell, dir);

      //create an empty file
      Path file = new Path(chmodDir, "file");
      TestDFSShell.writeFile(fs, file);

      //test octal mode
      confirmPermissionChange("644", "rw-r--r--", fs, shell, file);

      //test recursive
      runCmd(shell, "-chmod", "-R", "a+rwX", chmodDir);
      assertEquals("rwxrwxrwx",
          fs.getFileStatus(dir).getPermission().toString());
      assertEquals("rw-rw-rw-",
          fs.getFileStatus(file).getPermission().toString());

      // Skip "sticky bit" tests on Windows.
      //
      if (!Path.WINDOWS) {
        // test sticky bit on directories
        Path dir2 = new Path(dir, "stickybit");
        fs.mkdirs(dir2);
        LOG.info("Testing sticky bit on: " + dir2);
        LOG.info("Sticky bit directory initial mode: " +
            fs.getFileStatus(dir2).getPermission());

        confirmPermissionChange("u=rwx,g=rx,o=rx", "rwxr-xr-x", fs, shell, dir2);

        confirmPermissionChange("+t", "rwxr-xr-t", fs, shell, dir2);

        confirmPermissionChange("-t", "rwxr-xr-x", fs, shell, dir2);

        confirmPermissionChange("=t", "--------T", fs, shell, dir2);

        confirmPermissionChange("0000", "---------", fs, shell, dir2);

        confirmPermissionChange("1666", "rw-rw-rwT", fs, shell, dir2);

        confirmPermissionChange("777", "rwxrwxrwt", fs, shell, dir2);

        fs.delete(dir2, true);
      } else {
        LOG.info("Skipped sticky bit tests on Windows");
      }

      fs.delete(dir, true);

    } finally {
      try {
        fs.close();
        shell.close();
      } catch (IOException ignored) {}
    }
  }

  // Apply a new permission to a path and confirm that the new permission
  // is the one you were expecting
  private void confirmPermissionChange(String toApply, String expected,
      FileSystem fs, FsShell shell, Path dir2) throws IOException {
    LOG.info("Confirming permission change of " + toApply + " to " + expected);
    runCmd(shell, "-chmod", toApply, dir2.toString());

    String result = fs.getFileStatus(dir2).getPermission().toString();

    LOG.info("Permission change result: " + result);
    assertEquals(expected, result);
  }
   
  private void confirmOwner(String owner, String group, 
                            FileSystem fs, Path... paths) throws IOException {
    for(Path path : paths) {
      if (owner != null) {
        assertEquals(owner, fs.getFileStatus(path).getOwner());
      }
      if (group != null) {
        assertEquals(group, fs.getFileStatus(path).getGroup());
      }
    }
  }
  
  @Test (timeout = 30000)
  public void testFilePermissions() throws IOException {
    Configuration conf = new HdfsConfiguration();
    
    //test chmod on local fs
    FileSystem fs = FileSystem.getLocal(conf);
    testChmod(conf, fs, 
              (new File(TEST_ROOT_DIR, "chmodTest")).getAbsolutePath());
    
    conf.set(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, "true");
    
    //test chmod on DFS
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fs = cluster.getFileSystem();
    testChmod(conf, fs, "/tmp/chmodTest");
    
    // test chown and chgrp on DFS:
    
    FsShell shell = new FsShell();
    shell.setConf(conf);
    fs = cluster.getFileSystem();
    
    /* For dfs, I am the super user and I can change owner of any file to
     * anything. "-R" option is already tested by chmod test above.
     */
    
    String file = "/tmp/chownTest";
    Path path = new Path(file);
    Path parent = new Path("/tmp");
    Path root = new Path("/");
    TestDFSShell.writeFile(fs, path);
    
    runCmd(shell, "-chgrp", "-R", "herbivores", "/*", "unknownFile*");
    confirmOwner(null, "herbivores", fs, parent, path);
    
    runCmd(shell, "-chgrp", "mammals", file);
    confirmOwner(null, "mammals", fs, path);
    
    runCmd(shell, "-chown", "-R", ":reptiles", "/");
    confirmOwner(null, "reptiles", fs, root, parent, path);
    
    runCmd(shell, "-chown", "python:", "/nonExistentFile", file);
    confirmOwner("python", "reptiles", fs, path);

    runCmd(shell, "-chown", "-R", "hadoop:toys", "unknownFile", "/");
    confirmOwner("hadoop", "toys", fs, root, parent, path);
    
    // Test different characters in names

    runCmd(shell, "-chown", "hdfs.user", file);
    confirmOwner("hdfs.user", null, fs, path);
    
    runCmd(shell, "-chown", "_Hdfs.User-10:_hadoop.users--", file);
    confirmOwner("_Hdfs.User-10", "_hadoop.users--", fs, path);
    
    runCmd(shell, "-chown", "hdfs/hadoop-core@apache.org:asf-projects", file);
    confirmOwner("hdfs/hadoop-core@apache.org", "asf-projects", fs, path);
    
    runCmd(shell, "-chgrp", "hadoop-core@apache.org/100", file);
    confirmOwner(null, "hadoop-core@apache.org/100", fs, path);
    
    cluster.shutdown();
  }
  /**
   * Tests various options of DFSShell.
   */
  @Test (timeout = 120000)
  public void testDFSShell() throws IOException {
    Configuration conf = new HdfsConfiguration();
    /* This tests some properties of ChecksumFileSystem as well.
     * Make sure that we create ChecksumDFS */
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
            fs instanceof DistributedFileSystem);
    DistributedFileSystem fileSys = (DistributedFileSystem)fs;
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
      Path myFile2 = new Path("/test/mkdirs/myFile2");      
      writeFile(fileSys, myFile2);
      assertTrue(fileSys.exists(myFile2));

      // Verify that rm with a pattern
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile*";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
        assertFalse(fileSys.exists(myFile));
        assertFalse(fileSys.exists(myFile2));

        //re-create the files for other tests
        writeFile(fileSys, myFile);
        assertTrue(fileSys.exists(myFile));
        writeFile(fileSys, myFile2);
        assertTrue(fileSys.exists(myFile2));
      }

      // Verify that we can read the file
      {
        String[] args = new String[3];
        args[0] = "-cat";
        args[1] = "/test/mkdirs/myFile";
        args[2] = "/test/mkdirs/myFile2";        
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run: " +
                             StringUtils.stringifyException(e)); 
        }
        assertTrue(val == 0);
      }
      fileSys.delete(myFile2, true);

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
        String[] args;
        int val;

        args = new String[3];
        args[0] = "-test";
        args[1] = "-e";
        args[2] = "/test/mkdirs/noFileHere";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(1, val);

        args[1] = "-z";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(1, val);

        args = new String[2];
        args[0] = "-touchz";
        args[1] = "/test/mkdirs/isFileHere";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(0, val);

        args = new String[2];
        args[0] = "-touchz";
        args[1] = "/test/mkdirs/thisDirNotExists/isFileHere";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(1, val);


        args = new String[3];
        args[0] = "-test";
        args[1] = "-e";
        args[2] = "/test/mkdirs/isFileHere";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(0, val);

        args[1] = "-d";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(1, val);

        args[1] = "-z";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(0, val);
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
        assertEquals(0, val);

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
        assertEquals(1, val);

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
        assertEquals(0, val);
      }

      // Verify -test -f negative case (missing file)
      {
        String[] args = new String[3];
        args[0] = "-test";
        args[1] = "-f";
        args[2] = "/test/mkdirs/noFileHere";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(1, val);
      }

      // Verify -test -f negative case (directory rather than file)
      {
        String[] args = new String[3];
        args[0] = "-test";
        args[1] = "-f";
        args[2] = "/test/mkdirs";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(1, val);
      }

      // Verify -test -f positive case
      {
        writeFile(fileSys, myFile);
        assertTrue(fileSys.exists(myFile));

        String[] args = new String[3];
        args[0] = "-test";
        args[1] = "-f";
        args[2] = myFile.toString();
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(0, val);
      }

      // Verify -test -s negative case (missing file)
      {
        String[] args = new String[3];
        args[0] = "-test";
        args[1] = "-s";
        args[2] = "/test/mkdirs/noFileHere";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(1, val);
      }

      // Verify -test -s negative case (zero length file)
      {
        String[] args = new String[3];
        args[0] = "-test";
        args[1] = "-s";
        args[2] = "/test/mkdirs/isFileHere";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(1, val);
      }

      // Verify -test -s positive case (nonzero length file)
      {
        String[] args = new String[3];
        args[0] = "-test";
        args[1] = "-s";
        args[2] = myFile.toString();
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertEquals(0, val);
      }

    } finally {
      try {
        fileSys.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }

  static List<File> getBlockFiles(MiniDFSCluster cluster) throws IOException {
    List<File> files = new ArrayList<File>();
    List<DataNode> datanodes = cluster.getDataNodes();
    String poolId = cluster.getNamesystem().getBlockPoolId();
    List<Map<DatanodeStorage, BlockListAsLongs>> blocks = cluster.getAllBlockReports(poolId);
    for(int i = 0; i < blocks.size(); i++) {
      DataNode dn = datanodes.get(i);
      Map<DatanodeStorage, BlockListAsLongs> map = blocks.get(i);
      for(Map.Entry<DatanodeStorage, BlockListAsLongs> e : map.entrySet()) {
        for(Block b : e.getValue()) {
          files.add(DataNodeTestUtils.getFile(dn, poolId, b.getBlockId()));
        }
      }        
    }
    return files;
  }

  static void corrupt(List<File> files) throws IOException {
    for(File f : files) {
      StringBuilder content = new StringBuilder(DFSTestUtil.readFile(f));
      char c = content.charAt(0);
      content.setCharAt(0, ++c);
      PrintWriter out = new PrintWriter(f);
      out.print(content);
      out.flush();
      out.close();      
    }
  }

  static interface TestGetRunner {
    String run(int exitcode, String... options) throws IOException;
  }

  @Test (timeout = 30000)
  public void testRemoteException() throws Exception {
    UserGroupInformation tmpUGI = 
      UserGroupInformation.createUserForTesting("tmpname", new String[] {"mygroup"});
    MiniDFSCluster dfs = null;
    PrintStream bak = null;
    try {
      final Configuration conf = new HdfsConfiguration();
      dfs = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem fs = dfs.getFileSystem();
      Path p = new Path("/foo");
      fs.mkdirs(p);
      fs.setPermission(p, new FsPermission((short)0700));
      bak = System.err;
      
      tmpUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          FsShell fshell = new FsShell(conf);
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          PrintStream tmp = new PrintStream(out);
          System.setErr(tmp);
          String[] args = new String[2];
          args[0] = "-ls";
          args[1] = "/foo";
          int ret = ToolRunner.run(fshell, args);
          assertEquals("returned should be 1", 1, ret);
          String str = out.toString();
          assertTrue("permission denied printed", 
                     str.indexOf("Permission denied") != -1);
          out.reset();           
          return null;
        }
      });
    } finally {
      if (bak != null) {
        System.setErr(bak);
      }
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
  
  @Test (timeout = 30000)
  public void testGet() throws IOException {
    GenericTestUtils.setLogLevel(FSInputChecker.LOG, Level.ALL);

    final String fname = "testGet.txt";
    Path root = new Path("/test/get");
    final Path remotef = new Path(root, fname);
    final Configuration conf = new HdfsConfiguration();
    // Set short retry timeouts so this test runs faster
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    TestGetRunner runner = new TestGetRunner() {
    	private int count = 0;
    	private final FsShell shell = new FsShell(conf);

    	public String run(int exitcode, String... options) throws IOException {
    	  String dst = new File(TEST_ROOT_DIR, fname + ++count)
            .getAbsolutePath();
    	  String[] args = new String[options.length + 3];
    	  args[0] = "-get"; 
    	  args[args.length - 2] = remotef.toString();
    	  args[args.length - 1] = dst;
    	  for(int i = 0; i < options.length; i++) {
    	    args[i + 1] = options[i];
    	  }
    	  show("args=" + Arrays.asList(args));

    	  try {
    	    assertEquals(exitcode, shell.run(args));
    	  } catch (Exception e) {
    	    assertTrue(StringUtils.stringifyException(e), false); 
    	  }
    	  return exitcode == 0? DFSTestUtil.readFile(new File(dst)): null; 
    	}
    };

    File localf = createLocalFile(new File(TEST_ROOT_DIR, fname));
    MiniDFSCluster cluster = null;
    DistributedFileSystem dfs = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true)
        .build();
      dfs = cluster.getFileSystem();

      mkdir(dfs, root);
      dfs.copyFromLocalFile(false, false, new Path(localf.getPath()), remotef);
      String localfcontent = DFSTestUtil.readFile(localf);

      assertEquals(localfcontent, runner.run(0));
      assertEquals(localfcontent, runner.run(0, "-ignoreCrc"));

      // find block files to modify later
      List<File> files = getBlockFiles(cluster);

      // Shut down cluster and then corrupt the block files by overwriting a
      // portion with junk data.  We must shut down the cluster so that threads
      // in the data node do not hold locks on the block files while we try to
      // write into them.  Particularly on Windows, the data node's use of the
      // FileChannel.transferTo method can cause block files to be memory mapped
      // in read-only mode during the transfer to a client, and this causes a
      // locking conflict.  The call to shutdown the cluster blocks until all
      // DataXceiver threads exit, preventing this problem.
      dfs.close();
      cluster.shutdown();

      show("files=" + files);
      corrupt(files);

      // Start the cluster again, but do not reformat, so prior files remain.
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(false)
        .build();
      dfs = cluster.getFileSystem();

      assertEquals(null, runner.run(1));
      String corruptedcontent = runner.run(0, "-ignoreCrc");
      assertEquals(localfcontent.substring(1), corruptedcontent.substring(1));
      assertEquals(localfcontent.charAt(0)+1, corruptedcontent.charAt(0));
    } finally {
      if (null != dfs) {
        try {
          dfs.close();
        } catch (Exception e) {
        }
      }
      if (null != cluster) {
        cluster.shutdown();
      }
      localf.delete();
    }
  }

  @Test (timeout = 30000)
  public void testLsr() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    DistributedFileSystem dfs = cluster.getFileSystem();

    try {
      final String root = createTree(dfs, "lsr");
      dfs.mkdirs(new Path(root, "zzz"));
      
      runLsr(new FsShell(conf), root, 0);
      
      final Path sub = new Path(root, "sub");
      dfs.setPermission(sub, new FsPermission((short)0));

      final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      final String tmpusername = ugi.getShortUserName() + "1";
      UserGroupInformation tmpUGI = UserGroupInformation.createUserForTesting(
          tmpusername, new String[] {tmpusername});
      String results = tmpUGI.doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws Exception {
          return runLsr(new FsShell(conf), root, 1);
        }
      });
      assertTrue(results.contains("zzz"));
    } finally {
      cluster.shutdown();
    }
  }

  private static String runLsr(final FsShell shell, String root, int returnvalue
      ) throws Exception {
    System.out.println("root=" + root + ", returnvalue=" + returnvalue);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream(); 
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      assertEquals(returnvalue, shell.run(new String[]{"-lsr", root}));
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("results:\n" + results);
    return results;
  }
  
  /**
   * default setting is file:// which is not a DFS
   * so DFSAdmin should throw and catch InvalidArgumentException
   * and return -1 exit code.
   * @throws Exception
   */
  @Test (timeout = 30000)
  public void testInvalidShell() throws Exception {
    Configuration conf = new Configuration(); // default FS (non-DFS)
    DFSAdmin admin = new DFSAdmin();
    admin.setConf(conf);
    int res = admin.run(new String[] {"-refreshNodes"});
    assertEquals("expected to fail -1", res , -1);
  }
  
  // Preserve Copy Option is -ptopxa (timestamps, ownership, permission, XATTR,
  // ACLs)
  @Test (timeout = 120000)
  public void testCopyCommandsWithPreserveOption() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .format(true).build();
    FsShell shell = null;
    FileSystem fs = null;
    final String testdir = "/tmp/TestDFSShell-testCopyCommandsWithPreserveOption-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      fs = cluster.getFileSystem();
      fs.mkdirs(hdfsTestDir);
      Path src = new Path(hdfsTestDir, "srcfile");
      fs.create(src).close();

      fs.setAcl(src, Lists.newArrayList(
          aclEntry(ACCESS, USER, ALL),
          aclEntry(ACCESS, USER, "foo", ALL),
          aclEntry(ACCESS, GROUP, READ_EXECUTE),
          aclEntry(ACCESS, GROUP, "bar", READ_EXECUTE),
          aclEntry(ACCESS, OTHER, EXECUTE)));

      FileStatus status = fs.getFileStatus(src);
      final long mtime = status.getModificationTime();
      final long atime = status.getAccessTime();
      final String owner = status.getOwner();
      final String group = status.getGroup();
      final FsPermission perm = status.getPermission();
      
      fs.setXAttr(src, USER_A1, USER_A1_VALUE);
      fs.setXAttr(src, TRUSTED_A1, TRUSTED_A1_VALUE);
      
      shell = new FsShell(conf);
      
      // -p
      Path target1 = new Path(hdfsTestDir, "targetfile1");
      String[] argv = new String[] { "-cp", "-p", src.toUri().toString(), 
          target1.toUri().toString() };
      int ret = ToolRunner.run(shell, argv);
      assertEquals("cp -p is not working", SUCCESS, ret);
      FileStatus targetStatus = fs.getFileStatus(target1);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      FsPermission targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      Map<String, byte[]> xattrs = fs.getXAttrs(target1);
      assertTrue(xattrs.isEmpty());
      List<AclEntry> acls = fs.getAclStatus(target1).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptop
      Path target2 = new Path(hdfsTestDir, "targetfile2");
      argv = new String[] { "-cp", "-ptop", src.toUri().toString(), 
          target2.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptop is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(target2);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = fs.getXAttrs(target2);
      assertTrue(xattrs.isEmpty());
      acls = fs.getAclStatus(target2).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopx
      Path target3 = new Path(hdfsTestDir, "targetfile3");
      argv = new String[] { "-cp", "-ptopx", src.toUri().toString(), 
          target3.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopx is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(target3);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = fs.getXAttrs(target3);
      assertEquals(xattrs.size(), 2);
      assertArrayEquals(USER_A1_VALUE, xattrs.get(USER_A1));
      assertArrayEquals(TRUSTED_A1_VALUE, xattrs.get(TRUSTED_A1));
      acls = fs.getAclStatus(target3).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopa
      Path target4 = new Path(hdfsTestDir, "targetfile4");
      argv = new String[] { "-cp", "-ptopa", src.toUri().toString(),
          target4.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopa is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(target4);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = fs.getXAttrs(target4);
      assertTrue(xattrs.isEmpty());
      acls = fs.getAclStatus(target4).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(fs.getAclStatus(src), fs.getAclStatus(target4));

      // -ptoa (verify -pa option will preserve permissions also)
      Path target5 = new Path(hdfsTestDir, "targetfile5");
      argv = new String[] { "-cp", "-ptoa", src.toUri().toString(),
          target5.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptoa is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(target5);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = fs.getXAttrs(target5);
      assertTrue(xattrs.isEmpty());
      acls = fs.getAclStatus(target5).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(fs.getAclStatus(src), fs.getAclStatus(target5));
    } finally {
      if (null != shell) {
        shell.close();
      }

      if (null != fs) {
        fs.delete(hdfsTestDir, true);
        fs.close();
      }
      cluster.shutdown();
    }
  }

  @Test (timeout = 120000)
  public void testCopyCommandsWithRawXAttrs() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
      numDataNodes(1).format(true).build();
    FsShell shell = null;
    FileSystem fs = null;
    final String testdir = "/tmp/TestDFSShell-testCopyCommandsWithRawXAttrs-"
      + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    final Path rawHdfsTestDir = new Path("/.reserved/raw" + testdir);
    try {
      fs = cluster.getFileSystem();
      fs.mkdirs(hdfsTestDir);
      final Path src = new Path(hdfsTestDir, "srcfile");
      final String rawSrcBase = "/.reserved/raw" + testdir;
      final Path rawSrc = new Path(rawSrcBase, "srcfile");
      fs.create(src).close();

      final Path srcDir = new Path(hdfsTestDir, "srcdir");
      final Path rawSrcDir = new Path("/.reserved/raw" + testdir, "srcdir");
      fs.mkdirs(srcDir);
      final Path srcDirFile = new Path(srcDir, "srcfile");
      final Path rawSrcDirFile =
              new Path("/.reserved/raw" + srcDirFile);
      fs.create(srcDirFile).close();

      final Path[] paths = { rawSrc, rawSrcDir, rawSrcDirFile };
      final String[] xattrNames = { USER_A1, RAW_A1 };
      final byte[][] xattrVals = { USER_A1_VALUE, RAW_A1_VALUE };

      for (int i = 0; i < paths.length; i++) {
        for (int j = 0; j < xattrNames.length; j++) {
          fs.setXAttr(paths[i], xattrNames[j], xattrVals[j]);
        }
      }

      shell = new FsShell(conf);

      /* Check that a file as the source path works ok. */
      doTestCopyCommandsWithRawXAttrs(shell, fs, src, hdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, fs, rawSrc, hdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, fs, src, rawHdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, fs, rawSrc, rawHdfsTestDir, true);

      /* Use a relative /.reserved/raw path. */
      final Path savedWd = fs.getWorkingDirectory();
      try {
        fs.setWorkingDirectory(new Path(rawSrcBase));
        final Path relRawSrc = new Path("../srcfile");
        final Path relRawHdfsTestDir = new Path("..");
        doTestCopyCommandsWithRawXAttrs(shell, fs, relRawSrc, relRawHdfsTestDir,
                true);
      } finally {
        fs.setWorkingDirectory(savedWd);
      }

      /* Check that a directory as the source path works ok. */
      doTestCopyCommandsWithRawXAttrs(shell, fs, srcDir, hdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, fs, rawSrcDir, hdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, fs, srcDir, rawHdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, fs, rawSrcDir, rawHdfsTestDir,
        true);

      /* Use relative in an absolute path. */
      final String relRawSrcDir = "./.reserved/../.reserved/raw/../raw" +
          testdir + "/srcdir";
      final String relRawDstDir = "./.reserved/../.reserved/raw/../raw" +
          testdir;
      doTestCopyCommandsWithRawXAttrs(shell, fs, new Path(relRawSrcDir),
          new Path(relRawDstDir), true);
    } finally {
      if (null != shell) {
        shell.close();
      }

      if (null != fs) {
        fs.delete(hdfsTestDir, true);
        fs.close();
      }
      cluster.shutdown();
    }
  }

  private void doTestCopyCommandsWithRawXAttrs(FsShell shell, FileSystem fs,
      Path src, Path hdfsTestDir, boolean expectRaw) throws Exception {
    Path target;
    boolean srcIsRaw;
    if (src.isAbsolute()) {
      srcIsRaw = src.toString().contains("/.reserved/raw");
    } else {
      srcIsRaw = new Path(fs.getWorkingDirectory(), src).
          toString().contains("/.reserved/raw");
    }
    final boolean destIsRaw = hdfsTestDir.toString().contains("/.reserved/raw");
    final boolean srcDestMismatch = srcIsRaw ^ destIsRaw;

    // -p (possibly preserve raw if src & dst are both /.r/r */
    if (srcDestMismatch) {
      doCopyAndTest(shell, hdfsTestDir, src, "-p", ERROR);
    } else {
      target = doCopyAndTest(shell, hdfsTestDir, src, "-p", SUCCESS);
      checkXAttrs(fs, target, expectRaw, false);
    }

    // -px (possibly preserve raw, always preserve non-raw xattrs. */
    if (srcDestMismatch) {
      doCopyAndTest(shell, hdfsTestDir, src, "-px", ERROR);
    } else {
      target = doCopyAndTest(shell, hdfsTestDir, src, "-px", SUCCESS);
      checkXAttrs(fs, target, expectRaw, true);
    }

    // no args (possibly preserve raw, never preserve non-raw xattrs. */
    if (srcDestMismatch) {
      doCopyAndTest(shell, hdfsTestDir, src, null, ERROR);
    } else {
      target = doCopyAndTest(shell, hdfsTestDir, src, null, SUCCESS);
      checkXAttrs(fs, target, expectRaw, false);
    }
  }

  private Path doCopyAndTest(FsShell shell, Path dest, Path src,
      String cpArgs, int expectedExitCode) throws Exception {
    final Path target = new Path(dest, "targetfile" +
        counter.getAndIncrement());
    final String[] argv = cpArgs == null ?
        new String[] { "-cp",         src.toUri().toString(),
            target.toUri().toString() } :
        new String[] { "-cp", cpArgs, src.toUri().toString(),
            target.toUri().toString() };
    final int ret = ToolRunner.run(shell, argv);
    assertEquals("cp -p is not working", expectedExitCode, ret);
    return target;
  }

  private void checkXAttrs(FileSystem fs, Path target, boolean expectRaw,
      boolean expectVanillaXAttrs) throws Exception {
    final Map<String, byte[]> xattrs = fs.getXAttrs(target);
    int expectedCount = 0;
    if (expectRaw) {
      assertArrayEquals("raw.a1 has incorrect value",
          RAW_A1_VALUE, xattrs.get(RAW_A1));
      expectedCount++;
    }
    if (expectVanillaXAttrs) {
      assertArrayEquals("user.a1 has incorrect value",
          USER_A1_VALUE, xattrs.get(USER_A1));
      expectedCount++;
    }
    assertEquals("xattrs size mismatch", expectedCount, xattrs.size());
  }

  // verify cp -ptopxa option will preserve directory attributes.
  @Test (timeout = 120000)
  public void testCopyCommandsToDirectoryWithPreserveOption()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .format(true).build();
    FsShell shell = null;
    FileSystem fs = null;
    final String testdir =
        "/tmp/TestDFSShell-testCopyCommandsToDirectoryWithPreserveOption-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      fs = cluster.getFileSystem();
      fs.mkdirs(hdfsTestDir);
      Path srcDir = new Path(hdfsTestDir, "srcDir");
      fs.mkdirs(srcDir);

      fs.setAcl(srcDir, Lists.newArrayList(
          aclEntry(ACCESS, USER, ALL),
          aclEntry(ACCESS, USER, "foo", ALL),
          aclEntry(ACCESS, GROUP, READ_EXECUTE),
          aclEntry(DEFAULT, GROUP, "bar", READ_EXECUTE),
          aclEntry(ACCESS, OTHER, EXECUTE)));
      // set sticky bit
      fs.setPermission(srcDir,
          new FsPermission(ALL, READ_EXECUTE, EXECUTE, true));

      // Create a file in srcDir to check if modification time of
      // srcDir to be preserved after copying the file.
      // If cp -p command is to preserve modification time and then copy child
      // (srcFile), modification time will not be preserved.
      Path srcFile = new Path(srcDir, "srcFile");
      fs.create(srcFile).close();

      FileStatus status = fs.getFileStatus(srcDir);
      final long mtime = status.getModificationTime();
      final long atime = status.getAccessTime();
      final String owner = status.getOwner();
      final String group = status.getGroup();
      final FsPermission perm = status.getPermission();

      fs.setXAttr(srcDir, USER_A1, USER_A1_VALUE);
      fs.setXAttr(srcDir, TRUSTED_A1, TRUSTED_A1_VALUE);

      shell = new FsShell(conf);

      // -p
      Path targetDir1 = new Path(hdfsTestDir, "targetDir1");
      String[] argv = new String[] { "-cp", "-p", srcDir.toUri().toString(),
          targetDir1.toUri().toString() };
      int ret = ToolRunner.run(shell, argv);
      assertEquals("cp -p is not working", SUCCESS, ret);
      FileStatus targetStatus = fs.getFileStatus(targetDir1);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      FsPermission targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      Map<String, byte[]> xattrs = fs.getXAttrs(targetDir1);
      assertTrue(xattrs.isEmpty());
      List<AclEntry> acls = fs.getAclStatus(targetDir1).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptop
      Path targetDir2 = new Path(hdfsTestDir, "targetDir2");
      argv = new String[] { "-cp", "-ptop", srcDir.toUri().toString(),
          targetDir2.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptop is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(targetDir2);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = fs.getXAttrs(targetDir2);
      assertTrue(xattrs.isEmpty());
      acls = fs.getAclStatus(targetDir2).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopx
      Path targetDir3 = new Path(hdfsTestDir, "targetDir3");
      argv = new String[] { "-cp", "-ptopx", srcDir.toUri().toString(),
          targetDir3.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopx is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(targetDir3);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = fs.getXAttrs(targetDir3);
      assertEquals(xattrs.size(), 2);
      assertArrayEquals(USER_A1_VALUE, xattrs.get(USER_A1));
      assertArrayEquals(TRUSTED_A1_VALUE, xattrs.get(TRUSTED_A1));
      acls = fs.getAclStatus(targetDir3).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopa
      Path targetDir4 = new Path(hdfsTestDir, "targetDir4");
      argv = new String[] { "-cp", "-ptopa", srcDir.toUri().toString(),
          targetDir4.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopa is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(targetDir4);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = fs.getXAttrs(targetDir4);
      assertTrue(xattrs.isEmpty());
      acls = fs.getAclStatus(targetDir4).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(fs.getAclStatus(srcDir), fs.getAclStatus(targetDir4));

      // -ptoa (verify -pa option will preserve permissions also)
      Path targetDir5 = new Path(hdfsTestDir, "targetDir5");
      argv = new String[] { "-cp", "-ptoa", srcDir.toUri().toString(),
          targetDir5.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptoa is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(targetDir5);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = fs.getXAttrs(targetDir5);
      assertTrue(xattrs.isEmpty());
      acls = fs.getAclStatus(targetDir5).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(fs.getAclStatus(srcDir), fs.getAclStatus(targetDir5));
    } finally {
      if (shell != null) {
        shell.close();
      }
      if (fs != null) {
        fs.delete(hdfsTestDir, true);
        fs.close();
      }
      cluster.shutdown();
    }
  }

  // Verify cp -pa option will preserve both ACL and sticky bit.
  @Test (timeout = 120000)
  public void testCopyCommandsPreserveAclAndStickyBit() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .format(true).build();
    FsShell shell = null;
    FileSystem fs = null;
    final String testdir =
        "/tmp/TestDFSShell-testCopyCommandsPreserveAclAndStickyBit-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      fs = cluster.getFileSystem();
      fs.mkdirs(hdfsTestDir);
      Path src = new Path(hdfsTestDir, "srcfile");
      fs.create(src).close();

      fs.setAcl(src, Lists.newArrayList(
          aclEntry(ACCESS, USER, ALL),
          aclEntry(ACCESS, USER, "foo", ALL),
          aclEntry(ACCESS, GROUP, READ_EXECUTE),
          aclEntry(ACCESS, GROUP, "bar", READ_EXECUTE),
          aclEntry(ACCESS, OTHER, EXECUTE)));
      // set sticky bit
      fs.setPermission(src,
          new FsPermission(ALL, READ_EXECUTE, EXECUTE, true));

      FileStatus status = fs.getFileStatus(src);
      final long mtime = status.getModificationTime();
      final long atime = status.getAccessTime();
      final String owner = status.getOwner();
      final String group = status.getGroup();
      final FsPermission perm = status.getPermission();

      shell = new FsShell(conf);

      // -p preserves sticky bit and doesn't preserve ACL
      Path target1 = new Path(hdfsTestDir, "targetfile1");
      String[] argv = new String[] { "-cp", "-p", src.toUri().toString(),
          target1.toUri().toString() };
      int ret = ToolRunner.run(shell, argv);
      assertEquals("cp is not working", SUCCESS, ret);
      FileStatus targetStatus = fs.getFileStatus(target1);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      FsPermission targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      List<AclEntry> acls = fs.getAclStatus(target1).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopa preserves both sticky bit and ACL
      Path target2 = new Path(hdfsTestDir, "targetfile2");
      argv = new String[] { "-cp", "-ptopa", src.toUri().toString(),
          target2.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopa is not working", SUCCESS, ret);
      targetStatus = fs.getFileStatus(target2);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      acls = fs.getAclStatus(target2).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(fs.getAclStatus(src), fs.getAclStatus(target2));
    } finally {
      if (null != shell) {
        shell.close();
      }
      if (null != fs) {
        fs.delete(hdfsTestDir, true);
        fs.close();
      }
      cluster.shutdown();
    }
  }

  // force Copy Option is -f
  @Test (timeout = 30000)
  public void testCopyCommandsWithForceOption() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .format(true).build();
    FsShell shell = null;
    FileSystem fs = null;
    final File localFile = new File(TEST_ROOT_DIR, "testFileForPut");
    final String localfilepath = new Path(localFile.getAbsolutePath()).toUri().toString();
    final String testdir = "/tmp/TestDFSShell-testCopyCommandsWithForceOption-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      fs = cluster.getFileSystem();
      fs.mkdirs(hdfsTestDir);
      localFile.createNewFile();
      writeFile(fs, new Path(testdir, "testFileForPut"));
      shell = new FsShell();

      // Tests for put
      String[] argv = new String[] { "-put", "-f", localfilepath, testdir };
      int res = ToolRunner.run(shell, argv);
      assertEquals("put -f is not working", SUCCESS, res);

      argv = new String[] { "-put", localfilepath, testdir };
      res = ToolRunner.run(shell, argv);
      assertEquals("put command itself is able to overwrite the file", ERROR,
          res);

      // Tests for copyFromLocal
      argv = new String[] { "-copyFromLocal", "-f", localfilepath, testdir };
      res = ToolRunner.run(shell, argv);
      assertEquals("copyFromLocal -f is not working", SUCCESS, res);

      argv = new String[] { "-copyFromLocal", localfilepath, testdir };
      res = ToolRunner.run(shell, argv);
      assertEquals(
          "copyFromLocal command itself is able to overwrite the file", ERROR,
          res);

      // Tests for cp
      argv = new String[] { "-cp", "-f", localfilepath, testdir };
      res = ToolRunner.run(shell, argv);
      assertEquals("cp -f is not working", SUCCESS, res);

      argv = new String[] { "-cp", localfilepath, testdir };
      res = ToolRunner.run(shell, argv);
      assertEquals("cp command itself is able to overwrite the file", ERROR,
          res);
    } finally {
      if (null != shell)
        shell.close();

      if (localFile.exists())
        localFile.delete();

      if (null != fs) {
        fs.delete(hdfsTestDir, true);
        fs.close();
      }
      cluster.shutdown();
    }
  }

  // setrep for file and directory.
  @Test (timeout = 30000)
  public void testSetrep() throws Exception {

    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
                                                             .format(true).build();
    FsShell shell = null;
    FileSystem fs = null;

    final String testdir1 = "/tmp/TestDFSShell-testSetrep-" + counter.getAndIncrement();
    final String testdir2 = testdir1 + "/nestedDir";
    final Path hdfsFile1 = new Path(testdir1, "testFileForSetrep");
    final Path hdfsFile2 = new Path(testdir2, "testFileForSetrep");
    final Short oldRepFactor = new Short((short) 1);
    final Short newRepFactor = new Short((short) 3);
    try {
      String[] argv;
      cluster.waitActive();
      fs = cluster.getFileSystem();
      assertThat(fs.mkdirs(new Path(testdir2)), is(true));
      shell = new FsShell(conf);

      fs.create(hdfsFile1, true).close();
      fs.create(hdfsFile2, true).close();

      // Tests for setrep on a file.
      argv = new String[] { "-setrep", newRepFactor.toString(), hdfsFile1.toString() };
      assertThat(shell.run(argv), is(SUCCESS));
      assertThat(fs.getFileStatus(hdfsFile1).getReplication(), is(newRepFactor));
      assertThat(fs.getFileStatus(hdfsFile2).getReplication(), is(oldRepFactor));

      // Tests for setrep

      // Tests for setrep on a directory and make sure it is applied recursively.
      argv = new String[] { "-setrep", newRepFactor.toString(), testdir1 };
      assertThat(shell.run(argv), is(SUCCESS));
      assertThat(fs.getFileStatus(hdfsFile1).getReplication(), is(newRepFactor));
      assertThat(fs.getFileStatus(hdfsFile2).getReplication(), is(newRepFactor));

    } finally {
      if (shell != null) {
        shell.close();
      }

      cluster.shutdown();
    }
  }

  /**
   * Delete a file optionally configuring trash on the server and client.
   */
  private void deleteFileUsingTrash(
      boolean serverTrash, boolean clientTrash) throws Exception {
    // Run a cluster, optionally with trash enabled on the server
    Configuration serverConf = new HdfsConfiguration();
    if (serverTrash) {
      serverConf.setLong(FS_TRASH_INTERVAL_KEY, 1);
    }

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(serverConf)
      .numDataNodes(1).format(true).build();
    Configuration clientConf = new Configuration(serverConf);

    // Create a client, optionally with trash enabled
    if (clientTrash) {
      clientConf.setLong(FS_TRASH_INTERVAL_KEY, 1);
    } else {
      clientConf.setLong(FS_TRASH_INTERVAL_KEY, 0);
    }

    FsShell shell = new FsShell(clientConf);
    FileSystem fs = null;

    try {
      // Create and delete a file
      fs = cluster.getFileSystem();

      // Use a separate tmp dir for each invocation.
      final String testdir = "/tmp/TestDFSShell-deleteFileUsingTrash-" +
          counter.getAndIncrement();

      writeFile(fs, new Path(testdir, "foo"));
      final String testFile = testdir + "/foo";
      final String trashFile = shell.getCurrentTrashDir() + "/" + testFile;
      String[] argv = new String[] { "-rm", testFile };
      int res = ToolRunner.run(shell, argv);
      assertEquals("rm failed", 0, res);

      if (serverTrash) {
        // If the server config was set we should use it unconditionally
        assertTrue("File not in trash", fs.exists(new Path(trashFile)));
      } else if (clientTrash) {
        // If the server config was not set but the client config was
        // set then we should use it
        assertTrue("File not in trashed", fs.exists(new Path(trashFile)));
      } else {
        // If neither was set then we should not have trashed the file
        assertFalse("File was not removed", fs.exists(new Path(testFile)));
        assertFalse("File was trashed", fs.exists(new Path(trashFile)));
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test (timeout = 300000)
  public void testAppendToFile() throws Exception {
    final int inputFileLength = 1024 * 1024;
    File testRoot = new File(TEST_ROOT_DIR, "testAppendtoFileDir");
    testRoot.mkdirs();

    File file1 = new File(testRoot, "file1");
    File file2 = new File(testRoot, "file2");
    createLocalFileWithRandomData(inputFileLength, file1);
    createLocalFileWithRandomData(inputFileLength, file2);

    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    try {
      FileSystem dfs = cluster.getFileSystem();
      assertTrue("Not a HDFS: " + dfs.getUri(),
                 dfs instanceof DistributedFileSystem);

      // Run appendToFile once, make sure that the target file is
      // created and is of the right size.
      Path remoteFile = new Path("/remoteFile");
      FsShell shell = new FsShell();
      shell.setConf(conf);
      String[] argv = new String[] {
          "-appendToFile", file1.toString(), file2.toString(), remoteFile.toString() };
      int res = ToolRunner.run(shell, argv);
      assertThat(res, is(0));
      assertThat(dfs.getFileStatus(remoteFile).getLen(), is((long) inputFileLength * 2));

      // Run the command once again and make sure that the target file
      // size has been doubled.
      res = ToolRunner.run(shell, argv);
      assertThat(res, is(0));
      assertThat(dfs.getFileStatus(remoteFile).getLen(), is((long) inputFileLength * 4));
    } finally {
      cluster.shutdown();
    }
  }

  @Test (timeout = 300000)
  public void testAppendToFileBadArgs() throws Exception {
    final int inputFileLength = 1024 * 1024;
    File testRoot = new File(TEST_ROOT_DIR, "testAppendToFileBadArgsDir");
    testRoot.mkdirs();

    File file1 = new File(testRoot, "file1");
    createLocalFileWithRandomData(inputFileLength, file1);

    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    try {
      FileSystem dfs = cluster.getFileSystem();
      assertTrue("Not a HDFS: " + dfs.getUri(),
                 dfs instanceof DistributedFileSystem);

      // Run appendToFile with insufficient arguments.
      FsShell shell = new FsShell();
      shell.setConf(conf);
      String[] argv = new String[] {
          "-appendToFile", file1.toString() };
      int res = ToolRunner.run(shell, argv);
      assertThat(res, not(0));

      // Mix stdin with other input files. Must fail.
      Path remoteFile = new Path("/remoteFile");
      argv = new String[] {
          "-appendToFile", file1.toString(), "-", remoteFile.toString() };
      res = ToolRunner.run(shell, argv);
      assertThat(res, not(0));
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test (timeout = 30000)
  public void testSetXAttrPermission() throws Exception {
    UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] {"mygroup"});
    MiniDFSCluster cluster = null;
    PrintStream bak = null;
    try {
      final Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      
      FileSystem fs = cluster.getFileSystem();
      Path p = new Path("/foo");
      fs.mkdirs(p);
      bak = System.err;
      
      final FsShell fshell = new FsShell(conf);
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setErr(new PrintStream(out));
      
      // No permission to write xattr
      fs.setPermission(p, new FsPermission((short) 0700));
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          int ret = ToolRunner.run(fshell, new String[]{
              "-setfattr", "-n", "user.a1", "-v", "1234", "/foo"});
          assertEquals("Returned should be 1", 1, ret);
          String str = out.toString();
          assertTrue("Permission denied printed", 
              str.indexOf("Permission denied") != -1);
          out.reset();
          return null;
        }
      });
      
      int ret = ToolRunner.run(fshell, new String[]{
          "-setfattr", "-n", "user.a1", "-v", "1234", "/foo"});
      assertEquals("Returned should be 0", 0, ret);
      out.reset();
      
      // No permission to read and remove
      fs.setPermission(p, new FsPermission((short) 0750));
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // Read
          int ret = ToolRunner.run(fshell, new String[]{
              "-getfattr", "-n", "user.a1", "/foo"});
          assertEquals("Returned should be 1", 1, ret);
          String str = out.toString();
          assertTrue("Permission denied printed",
              str.indexOf("Permission denied") != -1);
          out.reset();           
          // Remove
          ret = ToolRunner.run(fshell, new String[]{
              "-setfattr", "-x", "user.a1", "/foo"});
          assertEquals("Returned should be 1", 1, ret);
          str = out.toString();
          assertTrue("Permission denied printed",
              str.indexOf("Permission denied") != -1);
          out.reset();  
          return null;
        }
      });
    } finally {
      if (bak != null) {
        System.setErr(bak);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /* HDFS-6413 xattr names erroneously handled as case-insensitive */
  @Test (timeout = 30000)
  public void testSetXAttrCaseSensitivity() throws Exception {
    UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] {"mygroup"});
    MiniDFSCluster cluster = null;
    PrintStream bak = null;
    try {
      final Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      FileSystem fs = cluster.getFileSystem();
      Path p = new Path("/mydir");
      fs.mkdirs(p);
      bak = System.err;

      final FsShell fshell = new FsShell(conf);
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));

      doSetXattr(out, fshell,
        new String[] {"-setfattr", "-n", "User.Foo", "/mydir"},
        new String[] {"-getfattr", "-d", "/mydir"},
        new String[] {"user.Foo"},
        new String[] {});

      doSetXattr(out, fshell,
        new String[] {"-setfattr", "-n", "user.FOO", "/mydir"},
        new String[] {"-getfattr", "-d", "/mydir"},
        new String[] {"user.Foo", "user.FOO"},
        new String[] {});

      doSetXattr(out, fshell,
        new String[] {"-setfattr", "-n", "USER.foo", "/mydir"},
        new String[] {"-getfattr", "-d", "/mydir"},
        new String[] {"user.Foo", "user.FOO", "user.foo"},
        new String[] {});

      doSetXattr(out, fshell,
        new String[] {"-setfattr", "-n", "USER.fOo", "-v", "myval", "/mydir"},
        new String[] {"-getfattr", "-d", "/mydir"},
        new String[] {"user.Foo", "user.FOO", "user.foo", "user.fOo=\"myval\""},
        new String[] {"user.Foo=", "user.FOO=", "user.foo="});

      doSetXattr(out, fshell,
        new String[] {"-setfattr", "-x", "useR.foo", "/mydir"},
        new String[] {"-getfattr", "-d", "/mydir"},
        new String[] {"user.Foo", "user.FOO"},
        new String[] {"foo"});

      doSetXattr(out, fshell,
        new String[] {"-setfattr", "-x", "USER.FOO", "/mydir"},
        new String[] {"-getfattr", "-d", "/mydir"},
        new String[] {"user.Foo"},
        new String[] {"FOO"});

      doSetXattr(out, fshell,
        new String[] {"-setfattr", "-x", "useR.Foo", "/mydir"},
        new String[] {"-getfattr", "-n", "User.Foo", "/mydir"},
        new String[] {},
        new String[] {"Foo"});

    } finally {
      if (bak != null) {
        System.setOut(bak);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void doSetXattr(ByteArrayOutputStream out, FsShell fshell,
    String[] setOp, String[] getOp, String[] expectArr,
    String[] dontExpectArr) throws Exception {
    int ret = ToolRunner.run(fshell, setOp);
    out.reset();
    ret = ToolRunner.run(fshell, getOp);
    final String str = out.toString();
    for (int i = 0; i < expectArr.length; i++) {
      final String expect = expectArr[i];
      final StringBuilder sb = new StringBuilder
        ("Incorrect results from getfattr. Expected: ");
      sb.append(expect).append(" Full Result: ");
      sb.append(str);
      assertTrue(sb.toString(),
        str.indexOf(expect) != -1);
    }

    for (int i = 0; i < dontExpectArr.length; i++) {
      String dontExpect = dontExpectArr[i];
      final StringBuilder sb = new StringBuilder
        ("Incorrect results from getfattr. Didn't Expect: ");
      sb.append(dontExpect).append(" Full Result: ");
      sb.append(str);
      assertTrue(sb.toString(),
        str.indexOf(dontExpect) == -1);
    }
    out.reset();
  }

  /**
   *
   * Test to make sure that user namespace xattrs can be set only if path has
   * access and for sticky directorries, only owner/privileged user can write.
   * Trusted namespace xattrs can be set only with privileged users.
   *
   * As user1: Create a directory (/foo) as user1, chown it to user1 (and
   * user1's group), grant rwx to "other".
   *
   * As user2: Set an xattr (should pass with path access).
   *
   * As user1: Set an xattr (should pass).
   *
   * As user2: Read the xattr (should pass). Remove the xattr (should pass with
   * path access).
   *
   * As user1: Read the xattr (should pass). Remove the xattr (should pass).
   * 
   * As user1: Change permissions only to owner
   * 
   * As User2: Set an Xattr (Should fail set with no path access) Remove an
   * Xattr (Should fail with no path access)
   * 
   * As SuperUser: Set an Xattr with Trusted (Should pass)
   */
  @Test (timeout = 30000)
  public void testSetXAttrPermissionAsDifferentOwner() throws Exception {
    final String USER1 = "user1";
    final String GROUP1 = "supergroup";
    final UserGroupInformation user1 = UserGroupInformation.
        createUserForTesting(USER1, new String[] {GROUP1});
    final UserGroupInformation user2 = UserGroupInformation.
        createUserForTesting("user2", new String[] {"mygroup2"});
    final UserGroupInformation SUPERUSER = UserGroupInformation.getCurrentUser();
    MiniDFSCluster cluster = null;
    PrintStream bak = null;
    try {
      final Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      final FileSystem fs = cluster.getFileSystem();
      fs.setOwner(new Path("/"), USER1, GROUP1);
      bak = System.err;

      final FsShell fshell = new FsShell(conf);
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setErr(new PrintStream(out));

      //Test 1.  Let user1 be owner for /foo
      user1.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final int ret = ToolRunner.run(fshell, new String[]{
              "-mkdir", "/foo"});
          assertEquals("Return should be 0", 0, ret);
          out.reset();
          return null;
        }
      });

      //Test 2. Give access to others
      user1.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // Give access to "other"
          final int ret = ToolRunner.run(fshell, new String[]{
              "-chmod", "707", "/foo"});
          assertEquals("Return should be 0", 0, ret);
          out.reset();
          return null;
        }
      });

      // Test 3. Should be allowed to write xattr if there is a path access to
      // user (user2).
      user2.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final int ret = ToolRunner.run(fshell, new String[]{
              "-setfattr", "-n", "user.a1", "-v", "1234", "/foo"});
          assertEquals("Returned should be 0", 0, ret);
          out.reset();
          return null;
        }
      });

      //Test 4. There should be permission to write xattr for
      // the owning user with write permissions.
      user1.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final int ret = ToolRunner.run(fshell, new String[]{
              "-setfattr", "-n", "user.a1", "-v", "1234", "/foo"});
          assertEquals("Returned should be 0", 0, ret);
          out.reset();
          return null;
        }
      });

      // Test 5. There should be permission to read non-owning user (user2) if
      // there is path access to that user and also can remove.
      user2.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // Read
          int ret = ToolRunner.run(fshell, new String[] { "-getfattr", "-n",
              "user.a1", "/foo" });
          assertEquals("Returned should be 0", 0, ret);
          out.reset();
          // Remove
          ret = ToolRunner.run(fshell, new String[] { "-setfattr", "-x",
              "user.a1", "/foo" });
          assertEquals("Returned should be 0", 0, ret);
          out.reset();
          return null;
        }
      });

      // Test 6. There should be permission to read/remove for
      // the owning user with path access.
      user1.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          return null;
        }
      });
      
      // Test 7. Change permission to have path access only to owner(user1)
      user1.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // Give access to "other"
          final int ret = ToolRunner.run(fshell, new String[]{
              "-chmod", "700", "/foo"});
          assertEquals("Return should be 0", 0, ret);
          out.reset();
          return null;
        }
      });
      
      // Test 8. There should be no permissions to set for
      // the non-owning user with no path access.
      user2.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // set
          int ret = ToolRunner.run(fshell, new String[] { "-setfattr", "-n",
              "user.a2", "/foo" });
          assertEquals("Returned should be 1", 1, ret);
          final String str = out.toString();
          assertTrue("Permission denied printed",
              str.indexOf("Permission denied") != -1);
          out.reset();
          return null;
        }
      });

      // Test 9. There should be no permissions to remove for
      // the non-owning user with no path access.
      user2.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // set
          int ret = ToolRunner.run(fshell, new String[] { "-setfattr", "-x",
              "user.a2", "/foo" });
          assertEquals("Returned should be 1", 1, ret);
          final String str = out.toString();
          assertTrue("Permission denied printed",
              str.indexOf("Permission denied") != -1);
          out.reset();
          return null;
        }
      });
      
      // Test 10. Superuser should be allowed to set with trusted namespace
      SUPERUSER.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // set
          int ret = ToolRunner.run(fshell, new String[] { "-setfattr", "-n",
              "trusted.a3", "/foo" });
          assertEquals("Returned should be 0", 0, ret);
          out.reset();
          return null;
        }
      });
    } finally {
      if (bak != null) {
        System.setErr(bak);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
   * 1. Test that CLI throws an exception and returns non-0 when user does
   * not have permission to read an xattr.
   * 2. Test that CLI throws an exception and returns non-0 when a non-existent
   * xattr is requested.
   */
  @Test (timeout = 120000)
  public void testGetFAttrErrors() throws Exception {
    final UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] {"mygroup"});
    MiniDFSCluster cluster = null;
    PrintStream bakErr = null;
    try {
      final Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      final FileSystem fs = cluster.getFileSystem();
      final Path p = new Path("/foo");
      fs.mkdirs(p);
      bakErr = System.err;

      final FsShell fshell = new FsShell(conf);
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setErr(new PrintStream(out));

      // No permission for "other".
      fs.setPermission(p, new FsPermission((short) 0700));

      {
        final int ret = ToolRunner.run(fshell, new String[] {
            "-setfattr", "-n", "user.a1", "-v", "1234", "/foo"});
        assertEquals("Returned should be 0", 0, ret);
        out.reset();
      }

      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            int ret = ToolRunner.run(fshell, new String[] {
                "-getfattr", "-n", "user.a1", "/foo"});
            String str = out.toString();
            assertTrue("xattr value was incorrectly returned",
                str.indexOf("1234") == -1);
            out.reset();
            return null;
          }
        });

      {
        final int ret = ToolRunner.run(fshell, new String[]{
            "-getfattr", "-n", "user.nonexistent", "/foo"});
        String str = out.toString();
        assertTrue("xattr value was incorrectly returned",
          str.indexOf(
              "getfattr: At least one of the attributes provided was not found")
               >= 0);
        out.reset();
      }
    } finally {
      if (bakErr != null) {
        System.setErr(bakErr);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that the server trash configuration is respected when
   * the client configuration is not set.
   */
  @Test (timeout = 30000)
  public void testServerConfigRespected() throws Exception {
    deleteFileUsingTrash(true, false);
  }

  /**
   * Test that server trash configuration is respected even when the
   * client configuration is set.
   */
  @Test (timeout = 30000)
  public void testServerConfigRespectedWithClient() throws Exception {
    deleteFileUsingTrash(true, true);
  }

  /**
   * Test that the client trash configuration is respected when
   * the server configuration is not set.
   */
  @Test (timeout = 30000)
  public void testClientConfigRespected() throws Exception {
    deleteFileUsingTrash(false, true);
  }

  /**
   * Test that trash is disabled by default.
   */
  @Test (timeout = 30000)
  public void testNoTrashConfig() throws Exception {
    deleteFileUsingTrash(false, false);
  }
}
