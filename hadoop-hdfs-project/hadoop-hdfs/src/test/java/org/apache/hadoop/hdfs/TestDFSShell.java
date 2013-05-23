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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.security.Permission;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;

/**
 * This class tests commands from DFSShell.
 */
public class TestDFSShell {
  private static final Log LOG = LogFactory.getLog(TestDFSShell.class);
  
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
  public void testRecrusiveRm() throws IOException {
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
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
                fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
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
      assertTrue(returnString.contains("22"));
      assertTrue(returnString.contains("23"));
      
    } finally {
      try {dfs.close();} catch (Exception e) {}
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
  
  @Test (timeout = 30000)
  public void testURIPaths() throws Exception {
    Configuration srcConf = new HdfsConfiguration();
    Configuration dstConf = new HdfsConfiguration();
    MiniDFSCluster srcCluster =  null;
    MiniDFSCluster dstCluster = null;
    String bak = System.getProperty("test.build.data");
    try{
      srcCluster = new MiniDFSCluster.Builder(srcConf).numDataNodes(2).build();
      File nameDir = new File(new File(bak), "dfs_tmp_uri/");
      nameDir.mkdirs();
      System.setProperty("test.build.data", nameDir.toString());
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
      argv[1] = furi.toString();
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
      System.setProperty("test.build.data", bak);
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
      CompressionCodec codec = (CompressionCodec)
          ReflectionUtils.newInstance(BZip2Codec.class, conf);
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
    DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
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
     confirmPermissionChange( "644", "rw-r--r--", fs, shell, file);

     //test recursive
     runCmd(shell, "-chmod", "-R", "a+rwX", chmodDir);
     assertEquals("rwxrwxrwx",
                  fs.getFileStatus(dir).getPermission().toString()); 
     assertEquals("rw-rw-rw-",
                  fs.getFileStatus(file).getPermission().toString());

     // test sticky bit on directories
     Path dir2 = new Path(dir, "stickybit" );
     fs.mkdirs(dir2 );
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
    Iterable<Block>[] blocks = cluster.getAllBlockReports(poolId);
    for(int i = 0; i < blocks.length; i++) {
      DataNode dn = datanodes.get(i);
      for(Block b : blocks[i]) {
        files.add(DataNodeTestUtils.getFile(dn, poolId, b.getBlockId()));
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
    DFSTestUtil.setLogLevel2All(FSInputChecker.LOG);

    final String fname = "testGet.txt";
    Path root = new Path("/test/get");
    final Path remotef = new Path(root, fname);
    final Configuration conf = new HdfsConfiguration();

    TestGetRunner runner = new TestGetRunner() {
    	private int count = 0;
    	private FsShell shell = new FsShell(conf);

    	public String run(int exitcode, String... options) throws IOException {
    	  String dst = TEST_ROOT_DIR + "/" + fname+ ++count;
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
      dfs = (DistributedFileSystem)cluster.getFileSystem();

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
      dfs = (DistributedFileSystem)cluster.getFileSystem();

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
    DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();

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

  // force Copy Option is -f
  @Test (timeout = 30000)
  public void testCopyCommandsWithForceOption() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .format(true).build();
    FsShell shell = null;
    FileSystem fs = null;
    final File localFile = new File(TEST_ROOT_DIR, "testFileForPut");
    final String localfilepath = localFile.getAbsolutePath();
    final String testdir = TEST_ROOT_DIR + "/ForceTestDir";
    final Path hdfsTestDir = new Path(testdir);
    try {
      fs = cluster.getFileSystem();
      fs.mkdirs(hdfsTestDir);
      localFile.createNewFile();
      writeFile(fs, new Path(TEST_ROOT_DIR, "testFileForPut"));
      shell = new FsShell();

      // Tests for put
      String[] argv = new String[] { "-put", "-f", localfilepath, testdir };
      int res = ToolRunner.run(shell, argv);
      int SUCCESS = 0;
      int ERROR = 1;
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
      writeFile(fs, new Path(TEST_ROOT_DIR, "foo"));
      final String testFile = TEST_ROOT_DIR + "/foo";
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
