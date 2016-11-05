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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils.MaterializedReplica;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.rules.Timeout;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

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
  private static final int BLOCK_SIZE = 1024;

  private static MiniDFSCluster miniCluster;
  private static DistributedFileSystem dfs;

  @BeforeClass
  public static void setup() throws IOException {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    // set up the shared miniCluster directory so individual tests can launch
    // new clusters without conflict
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
        GenericTestUtils.getTestDir("TestDFSShell").getAbsolutePath());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);

    miniCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    miniCluster.waitActive();
    dfs = miniCluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() {
    if (miniCluster != null) {
      miniCluster.shutdown(true, true);
    }
  }

  @Rule
  public Timeout globalTimeout= new Timeout(30 * 1000); // 30s

  static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    out.writeBytes("dhruba: " + f);
    out.close();
    assertTrue(fs.exists(f));
    return f;
  }

  static Path writeByte(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    out.writeByte(1);
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

  static void rmr(FileSystem fs, Path p) throws IOException {
    assertTrue(fs.delete(p, true));
    assertFalse(fs.exists(p));
  }

  /** Create a local file whose content contains its full path. */
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
    //create a zero size file
    final File f1 = new File(TEST_ROOT_DIR, "f1");
    assertTrue(!f1.exists());
    assertTrue(f1.createNewFile());
    assertTrue(f1.exists());
    assertTrue(f1.isFile());
    assertEquals(0L, f1.length());

    //copy to remote
    final Path root = mkdir(dfs, new Path("/testZeroSizeFile/zeroSizeFile"));
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
  }

  @Test (timeout = 30000)
  public void testRecursiveRm() throws IOException {
    final Path parent = new Path("/testRecursiveRm", "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(child);
    try {
      dfs.delete(parent, false);
      fail("Should have failed because dir is not empty");
    } catch(IOException e) {
       //should have thrown an exception
    }
    dfs.delete(parent, true);
    assertFalse(dfs.exists(parent));
  }

  @Test (timeout = 30000)
  public void testDu() throws IOException {
    int replication = 2;
    PrintStream psBackup = System.out;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream psOut = new PrintStream(out);
    System.setOut(psOut);
    FsShell shell = new FsShell(dfs.getConf());

    try {
      final Path myPath = new Path("/testDu", "dir");
      assertTrue(dfs.mkdirs(myPath));
      assertTrue(dfs.exists(myPath));
      final Path myFile = new Path(myPath, "file");
      writeFile(dfs, myFile);
      assertTrue(dfs.exists(myFile));
      final Path myFile2 = new Path(myPath, "file2");
      writeFile(dfs, myFile2);
      assertTrue(dfs.exists(myFile2));
      Long myFileLength = dfs.getFileStatus(myFile).getLen();
      Long myFileDiskUsed = myFileLength * replication;
      Long myFile2Length = dfs.getFileStatus(myFile2).getLen();
      Long myFile2DiskUsed = myFile2Length * replication;

      String[] args = new String[2];
      args[0] = "-du";
      args[1] = myPath.toString();
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
      // Check if size matches as expected
      assertThat(returnString, containsString(myFileLength.toString()));
      assertThat(returnString, containsString(myFileDiskUsed.toString()));
      assertThat(returnString, containsString(myFile2Length.toString()));
      assertThat(returnString, containsString(myFile2DiskUsed.toString()));

      // Check that -du -s reports the state of the snapshot
      String snapshotName = "ss1";
      Path snapshotPath = new Path(myPath, ".snapshot/" + snapshotName);
      dfs.allowSnapshot(myPath);
      assertThat(dfs.createSnapshot(myPath, snapshotName), is(snapshotPath));
      assertThat(dfs.delete(myFile, false), is(true));
      assertThat(dfs.exists(myFile), is(false));

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
      Long combinedDiskUsed = myFileDiskUsed + myFile2DiskUsed;
      assertThat(returnString, containsString(combinedLength.toString()));
      assertThat(returnString, containsString(combinedDiskUsed.toString()));

      // Check if output is rendered properly with multiple input paths
      final Path myFile3 = new Path(myPath, "file3");
      writeByte(dfs, myFile3);
      assertTrue(dfs.exists(myFile3));
      args = new String[3];
      args[0] = "-du";
      args[1] = myFile3.toString();
      args[2] = myFile2.toString();
      val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals("Return code should be 0.", 0, val);
      returnString = out.toString();
      out.reset();
      assertTrue(returnString.contains("1   2   " + myFile3.toString()));
      assertTrue(returnString.contains("25  50  " + myFile2.toString()));
    } finally {
      System.setOut(psBackup);
    }
  }

  @Test (timeout = 180000)
  public void testDuSnapshots() throws IOException {
    final int replication = 2;
    final PrintStream psBackup = System.out;
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final PrintStream psOut = new PrintStream(out);
    final FsShell shell = new FsShell(dfs.getConf());

    try {
      System.setOut(psOut);
      final Path parent = new Path("/testDuSnapshots");
      final Path dir = new Path(parent, "dir");
      mkdir(dfs, dir);
      final Path file = new Path(dir, "file");
      writeFile(dfs, file);
      final Path file2 = new Path(dir, "file2");
      writeFile(dfs, file2);
      final Long fileLength = dfs.getFileStatus(file).getLen();
      final Long fileDiskUsed = fileLength * replication;
      final Long file2Length = dfs.getFileStatus(file2).getLen();
      final Long file2DiskUsed = file2Length * replication;

      /*
       * Construct dir as follows:
       * /test/dir/file   <- this will later be deleted after snapshot taken.
       * /test/dir/newfile <- this will be created after snapshot taken.
       * /test/dir/file2
       * Snapshot enabled on /test
       */

      // test -du on /test/dir
      int ret = -1;
      try {
        ret = shell.run(new String[] {"-du", dir.toString()});
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, ret);
      String returnString = out.toString();
      LOG.info("-du return is:\n" + returnString);
      // Check if size matches as expected
      assertTrue(returnString.contains(fileLength.toString()));
      assertTrue(returnString.contains(fileDiskUsed.toString()));
      assertTrue(returnString.contains(file2Length.toString()));
      assertTrue(returnString.contains(file2DiskUsed.toString()));
      out.reset();

      // take a snapshot, then remove file and add newFile
      final String snapshotName = "ss1";
      final Path snapshotPath = new Path(parent, ".snapshot/" + snapshotName);
      dfs.allowSnapshot(parent);
      assertThat(dfs.createSnapshot(parent, snapshotName), is(snapshotPath));
      rmr(dfs, file);
      final Path newFile = new Path(dir, "newfile");
      writeFile(dfs, newFile);
      final Long newFileLength = dfs.getFileStatus(newFile).getLen();
      final Long newFileDiskUsed = newFileLength * replication;

      // test -du -s on /test
      ret = -1;
      try {
        ret = shell.run(new String[] {"-du", "-s", parent.toString()});
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, ret);
      returnString = out.toString();
      LOG.info("-du -s return is:\n" + returnString);
      Long combinedLength = fileLength + file2Length + newFileLength;
      Long combinedDiskUsed = fileDiskUsed + file2DiskUsed + newFileDiskUsed;
      assertTrue(returnString.contains(combinedLength.toString()));
      assertTrue(returnString.contains(combinedDiskUsed.toString()));
      out.reset();

      // test -du on /test
      ret = -1;
      try {
        ret = shell.run(new String[] {"-du", parent.toString()});
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, ret);
      returnString = out.toString();
      LOG.info("-du return is:\n" + returnString);
      assertTrue(returnString.contains(combinedLength.toString()));
      assertTrue(returnString.contains(combinedDiskUsed.toString()));
      out.reset();

      // test -du -s -x on /test
      ret = -1;
      try {
        ret = shell.run(new String[] {"-du", "-s", "-x", parent.toString()});
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, ret);
      returnString = out.toString();
      LOG.info("-du -s -x return is:\n" + returnString);
      Long exludeSnapshotLength = file2Length + newFileLength;
      Long excludeSnapshotDiskUsed = file2DiskUsed + newFileDiskUsed;
      assertTrue(returnString.contains(exludeSnapshotLength.toString()));
      assertTrue(returnString.contains(excludeSnapshotDiskUsed.toString()));
      out.reset();

      // test -du -x on /test
      ret = -1;
      try {
        ret = shell.run(new String[] {"-du", "-x", parent.toString()});
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, ret);
      returnString = out.toString();
      LOG.info("-du -x return is:\n" + returnString);
      assertTrue(returnString.contains(exludeSnapshotLength.toString()));
      assertTrue(returnString.contains(excludeSnapshotDiskUsed.toString()));
      out.reset();
    } finally {
      System.setOut(psBackup);
    }
  }

  @Test (timeout = 180000)
  public void testCountSnapshots() throws IOException {
    final PrintStream psBackup = System.out;
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final PrintStream psOut = new PrintStream(out);
    System.setOut(psOut);
    final FsShell shell = new FsShell(dfs.getConf());

    try {
      final Path parent = new Path("/testCountSnapshots");
      final Path dir = new Path(parent, "dir");
      mkdir(dfs, dir);
      final Path file = new Path(dir, "file");
      writeFile(dfs, file);
      final Path file2 = new Path(dir, "file2");
      writeFile(dfs, file2);
      final long fileLength = dfs.getFileStatus(file).getLen();
      final long file2Length = dfs.getFileStatus(file2).getLen();
      final Path dir2 = new Path(parent, "dir2");
      mkdir(dfs, dir2);

      /*
       * Construct dir as follows:
       * /test/dir/file   <- this will later be deleted after snapshot taken.
       * /test/dir/newfile <- this will be created after snapshot taken.
       * /test/dir/file2
       * /test/dir2       <- this will later be deleted after snapshot taken.
       * Snapshot enabled on /test
       */

      // take a snapshot
      // then create /test/dir/newfile and remove /test/dir/file, /test/dir2
      final String snapshotName = "s1";
      final Path snapshotPath = new Path(parent, ".snapshot/" + snapshotName);
      dfs.allowSnapshot(parent);
      assertThat(dfs.createSnapshot(parent, snapshotName), is(snapshotPath));
      rmr(dfs, file);
      rmr(dfs, dir2);
      final Path newFile = new Path(dir, "new file");
      writeFile(dfs, newFile);
      final Long newFileLength = dfs.getFileStatus(newFile).getLen();

      // test -count on /test. Include header for easier debugging.
      int val = -1;
      try {
        val = shell.run(new String[] {"-count", "-v", parent.toString() });
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, val);
      String returnString = out.toString();
      LOG.info("-count return is:\n" + returnString);
      Scanner in = new Scanner(returnString);
      in.nextLine();
      assertEquals(3, in.nextLong()); //DIR_COUNT
      assertEquals(3, in.nextLong()); //FILE_COUNT
      assertEquals(fileLength + file2Length + newFileLength,
          in.nextLong()); //CONTENT_SIZE
      out.reset();

      // test -count -x on /test. Include header for easier debugging.
      val = -1;
      try {
        val =
            shell.run(new String[] {"-count", "-x", "-v", parent.toString()});
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, val);
      returnString = out.toString();
      LOG.info("-count -x return is:\n" + returnString);
      in = new Scanner(returnString);
      in.nextLine();
      assertEquals(2, in.nextLong()); //DIR_COUNT
      assertEquals(2, in.nextLong()); //FILE_COUNT
      assertEquals(file2Length + newFileLength, in.nextLong()); //CONTENT_SIZE
      out.reset();
    } finally {
      System.setOut(psBackup);
    }
  }

  @Test (timeout = 30000)
  public void testPut() throws IOException {
    // remove left over crc files:
    new File(TEST_ROOT_DIR, ".f1.crc").delete();
    new File(TEST_ROOT_DIR, ".f2.crc").delete();
    final File f1 = createLocalFile(new File(TEST_ROOT_DIR, "f1"));
    final File f2 = createLocalFile(new File(TEST_ROOT_DIR, "f2"));

    final Path root = mkdir(dfs, new Path("/testPut"));
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
    final Path destmultiple = mkdir(dfs, new Path(root, "putmultiple"));
    Path[] srcs = new Path[2];
    srcs[0] = new Path(f1.getPath());
    srcs[1] = new Path(f2.getPath());
    dfs.copyFromLocalFile(false, false, srcs, destmultiple);
    srcs[0] = new Path(destmultiple,"f1");
    srcs[1] = new Path(destmultiple,"f2");
    assertTrue(dfs.exists(srcs[0]));
    assertTrue(dfs.exists(srcs[1]));

    // move multiple files to destination directory
    final Path destmultiple2 = mkdir(dfs, new Path(root, "movemultiple"));
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
  }

  /** check command error outputs and exit statuses. */
  @Test (timeout = 30000)
  public void testErrOutPut() throws Exception {
    PrintStream bak = null;
    try {
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
      FsShell shell = new FsShell(dfs.getConf());
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
      dfs.mkdirs(new Path("/testdir"));
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
      OutputStream outtmp = dfs.create(testFile);
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
      argv[2] = "/no-such-dir/file";
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
      dfs.mkdirs(dfs.getHomeDirectory());
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
          .nameNodePort(ServerSocketUtil.waitForPort(
              HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT, 10))
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
    File bak = new File(PathUtils.getTestDir(getClass()), "testURIPaths");
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

  /**
   * Test that -tail displays last kilobyte of the file to stdout.
   */
  @Test (timeout = 30000)
  public void testTail() throws Exception {
    final int fileLen = 5 * BLOCK_SIZE;

    // create a text file with multiple KB bytes (and multiple blocks)
    final Path testFile = new Path("testTail", "file1");
    final String text = RandomStringUtils.randomAscii(fileLen);
    try (OutputStream pout = dfs.create(testFile)) {
      pout.write(text.getBytes());
    }
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));
    final String[] argv = new String[]{"-tail", testFile.toString()};
    final int ret = ToolRunner.run(new FsShell(dfs.getConf()), argv);

    assertEquals(Arrays.toString(argv) + " returned " + ret, 0, ret);
    assertEquals("-tail returned " + out.size() + " bytes data, expected 1KB",
        1024, out.size());
    // tailed out last 1KB of the file content
    assertArrayEquals("Tail output doesn't match input",
        text.substring(fileLen - 1024).getBytes(), out.toByteArray());
    out.reset();
  }

  /**
   * Test that -tail -f outputs appended data as the file grows.
   */
  @Test(timeout = 30000)
  public void testTailWithFresh() throws Exception {
    final Path testFile = new Path("testTailWithFresh", "file1");
    dfs.create(testFile);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));
    final Thread tailer = new Thread() {
      @Override
      public void run() {
        final String[] argv = new String[]{"-tail", "-f",
            testFile.toString()};
        try {
          ToolRunner.run(new FsShell(dfs.getConf()), argv);
        } catch (Exception e) {
          LOG.error("Client that tails the test file fails", e);
        } finally {
          out.reset();
        }
      }
    };
    tailer.start();
    // wait till the tailer is sleeping
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return tailer.getState() == Thread.State.TIMED_WAITING;
      }
    }, 100, 10000);

    final String text = RandomStringUtils.randomAscii(BLOCK_SIZE / 2);
    try (OutputStream pout = dfs.create(testFile)) {
      pout.write(text.getBytes());
    }
    // The tailer should eventually show the file contents
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return Arrays.equals(text.getBytes(), out.toByteArray());
      }
    }, 100, 10000);
  }

  @Test (timeout = 30000)
  public void testText() throws Exception {
    final Configuration conf = dfs.getConf();
    textTest(new Path("/texttest").makeQualified(dfs.getUri(),
          dfs.getWorkingDirectory()), conf);

    final FileSystem lfs = FileSystem.getLocal(conf);
    textTest(new Path(TEST_ROOT_DIR, "texttest").makeQualified(lfs.getUri(),
          lfs.getWorkingDirectory()), conf);
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

      // Test a plain text.
      OutputStream pout = fs.create(new Path(root, "file.txt"));
      writebytes = "bar".getBytes();
      pout.write(writebytes);
      pout.close();
      out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));
      argv = new String[2];
      argv[0] = "-text";
      argv[1] = new Path(root, "file.txt").toString();
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
    FsShell shell = new FsShell(dfs.getConf());

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
    FsShell shell = new FsShell(dfs.getConf());

    String root = createTree(dfs, "count");

    // Verify the counts
    runCount(root, 2, 4, shell);
    runCount(root + "2", 2, 1, shell);
    runCount(root + "2/f1", 0, 1, shell);
    runCount(root + "2/sub", 1, 0, shell);

    final FileSystem localfs = FileSystem.getLocal(dfs.getConf());
    Path localpath = new Path(TEST_ROOT_DIR, "testcount");
    localpath = localpath.makeQualified(localfs.getUri(),
        localfs.getWorkingDirectory());
    localfs.mkdirs(localpath);

    final String localstr = localpath.toString();
    System.out.println("localstr=" + localstr);
    runCount(localstr, 1, 0, shell);
    assertEquals(0, runCmd(shell, "-count", root, localstr));
  }

  @Test(timeout = 30000)
  public void testTotalSizeOfAllFiles() throws Exception {
    final Path root = new Path("/testTotalSizeOfAllFiles");
    dfs.mkdirs(root);
    // create file under root
    FSDataOutputStream File1 = dfs.create(new Path(root, "File1"));
    File1.write("hi".getBytes());
    File1.close();
    // create file under sub-folder
    FSDataOutputStream File2 = dfs.create(new Path(root, "Folder1/File2"));
    File2.write("hi".getBytes());
    File2.close();
    // getUsed() should return total length of all the files in Filesystem
    assertEquals(4, dfs.getUsed(root));
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
      System.setOut(oldOut);
      if (in!=null) in.close();
      IOUtils.closeStream(out);
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
        // sticky bit explicit set
        confirmPermissionChange("+t", "rwxr-xr-t", fs, shell, dir2);
        // sticky bit explicit reset
        confirmPermissionChange("-t", "rwxr-xr-x", fs, shell, dir2);
        confirmPermissionChange("=t", "--------T", fs, shell, dir2);
        // reset all permissions
        confirmPermissionChange("0000", "---------", fs, shell, dir2);
        // turn on rw permissions for all
        confirmPermissionChange("1666", "rw-rw-rwT", fs, shell, dir2);
        // sticky bit explicit set along with x permission
        confirmPermissionChange("1777", "rwxrwxrwt", fs, shell, dir2);
        // sticky bit explicit reset
        confirmPermissionChange("0777", "rwxrwxrwx", fs, shell, dir2);
        // sticky bit explicit set
        confirmPermissionChange("1777", "rwxrwxrwt", fs, shell, dir2);
        // sticky bit implicit reset
        confirmPermissionChange("777", "rwxrwxrwx", fs, shell, dir2);
        fs.delete(dir2, true);
      } else {
        LOG.info("Skipped sticky bit tests on Windows");
      }

      fs.delete(dir, true);

    } finally {
      try {
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
    fs = dfs;
    conf = dfs.getConf();
    testChmod(conf, fs, "/tmp/chmodTest");

    // test chown and chgrp on DFS:

    FsShell shell = new FsShell();
    shell.setConf(conf);

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
  }

  /**
   * Tests various options of DFSShell.
   */
  @Test (timeout = 120000)
  public void testDFSShell() throws Exception {
    /* This tests some properties of ChecksumFileSystem as well.
     * Make sure that we create ChecksumDFS */
    FsShell shell = new FsShell(dfs.getConf());

    // First create a new directory with mkdirs
    Path myPath = new Path("/testDFSShell/mkdirs");
    assertTrue(dfs.mkdirs(myPath));
    assertTrue(dfs.exists(myPath));
    assertTrue(dfs.mkdirs(myPath));

    // Second, create a file in that directory.
    Path myFile = new Path("/testDFSShell/mkdirs/myFile");
    writeFile(dfs, myFile);
    assertTrue(dfs.exists(myFile));
    Path myFile2 = new Path("/testDFSShell/mkdirs/myFile2");
    writeFile(dfs, myFile2);
    assertTrue(dfs.exists(myFile2));

    // Verify that rm with a pattern
    {
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = "/testDFSShell/mkdirs/myFile*";
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
      assertFalse(dfs.exists(myFile));
      assertFalse(dfs.exists(myFile2));

      //re-create the files for other tests
      writeFile(dfs, myFile);
      assertTrue(dfs.exists(myFile));
      writeFile(dfs, myFile2);
      assertTrue(dfs.exists(myFile2));
    }

    // Verify that we can read the file
    {
      String[] args = new String[3];
      args[0] = "-cat";
      args[1] = "/testDFSShell/mkdirs/myFile";
      args[2] = "/testDFSShell/mkdirs/myFile2";
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run: " +
                           StringUtils.stringifyException(e));
      }
      assertTrue(val == 0);
    }
    dfs.delete(myFile2, true);

    // Verify that we get an error while trying to read an nonexistent file
    {
      String[] args = new String[2];
      args[0] = "-cat";
      args[1] = "/testDFSShell/mkdirs/myFile1";
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
      args[1] = "/testDFSShell/mkdirs/myFile1";
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
      args[1] = "/testDFSShell/mkdirs/myFile";
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
      args[2] = "/testDFSShell/mkdirs/noFileHere";
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
      args[1] = "/testDFSShell/mkdirs/isFileHere";
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
      args[1] = "/testDFSShell/mkdirs/thisDirNotExists/isFileHere";
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
      args[2] = "/testDFSShell/mkdirs/isFileHere";
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
      args[1] = "/testDFSShell/dir1";
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
      args1[1] = "/testDFSShell/dir1";
      args1[2] = "/testDFSShell/dir1/dir2";
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
      args1[1] = "/testDFSShell/dir1";
      args1[2] = "/testDFSShell/dir1foo";
      val = -1;
      try {
        val = shell.run(args1);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
                           e.getLocalizedMessage());
      }
      assertEquals(0, val);

      // this should fail
      args1[0] = "-cp";
      args1[1] = "/";
      args1[2] = "/test";
      val = 0;
      try {
        val = shell.run(args1);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(1, val);
    }

    // Verify -test -f negative case (missing file)
    {
      String[] args = new String[3];
      args[0] = "-test";
      args[1] = "-f";
      args[2] = "/testDFSShell/mkdirs/noFileHere";
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
      args[2] = "/testDFSShell/mkdirs";
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
      writeFile(dfs, myFile);
      assertTrue(dfs.exists(myFile));

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
      args[2] = "/testDFSShell/mkdirs/noFileHere";
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
      args[2] = "/testDFSShell/mkdirs/isFileHere";
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

    // Verify -test -w/-r
    {
      Path permDir = new Path("/testDFSShell/permDir");
      Path permFile = new Path("/testDFSShell/permDir/permFile");
      mkdir(dfs, permDir);
      writeFile(dfs, permFile);

      // Verify -test -w positive case (dir exists and can write)
      final String[] wargs = new String[3];
      wargs[0] = "-test";
      wargs[1] = "-w";
      wargs[2] = permDir.toString();
      int val = -1;
      try {
        val = shell.run(wargs);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, val);

      // Verify -test -r positive case (file exists and can read)
      final String[] rargs = new String[3];
      rargs[0] = "-test";
      rargs[1] = "-r";
      rargs[2] = permFile.toString();
      try {
        val = shell.run(rargs);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertEquals(0, val);

      // Verify -test -r negative case (file exists but cannot read)
      runCmd(shell, "-chmod", "600", permFile.toString());

      UserGroupInformation smokeUser =
          UserGroupInformation.createUserForTesting("smokeUser",
              new String[] {"hadoop"});
      smokeUser.doAs(new PrivilegedExceptionAction<String>() {
          @Override
          public String run() throws Exception {
            FsShell shell = new FsShell(dfs.getConf());
            int exitCode = shell.run(rargs);
            assertEquals(1, exitCode);
            return null;
          }
        });

      // Verify -test -w negative case (dir exists but cannot write)
      runCmd(shell, "-chown", "-R", "not_allowed", permDir.toString());
      runCmd(shell, "-chmod", "-R", "700", permDir.toString());

      smokeUser.doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws Exception {
          FsShell shell = new FsShell(dfs.getConf());
          int exitCode = shell.run(wargs);
          assertEquals(1, exitCode);
          return null;
        }
      });

      // cleanup
      dfs.delete(permDir, true);
    }
  }

  private static List<MaterializedReplica> getMaterializedReplicas(
      MiniDFSCluster cluster) throws IOException {
    List<MaterializedReplica> replicas = new ArrayList<>();
    String poolId = cluster.getNamesystem().getBlockPoolId();
    List<Map<DatanodeStorage, BlockListAsLongs>> blocks =
        cluster.getAllBlockReports(poolId);
    for(int i = 0; i < blocks.size(); i++) {
      Map<DatanodeStorage, BlockListAsLongs> map = blocks.get(i);
      for(Map.Entry<DatanodeStorage, BlockListAsLongs> e : map.entrySet()) {
        for(Block b : e.getValue()) {
          replicas.add(cluster.getMaterializedReplica(i,
              new ExtendedBlock(poolId, b)));
        }
      }
    }
    return replicas;
  }

  private static void corrupt(
      List<MaterializedReplica> replicas, String content) throws IOException {
    StringBuilder sb = new StringBuilder(content);
    char c = content.charAt(0);
    sb.setCharAt(0, ++c);
    for(MaterializedReplica replica : replicas) {
      replica.corruptData(sb.toString().getBytes("UTF8"));
    }
  }

  static interface TestGetRunner {
    String run(int exitcode, String... options) throws IOException;
  }

  @Test (timeout = 30000)
  public void testRemoteException() throws Exception {
    UserGroupInformation tmpUGI =
      UserGroupInformation.createUserForTesting("tmpname", new String[] {"mygroup"});
    PrintStream bak = null;
    try {
      Path p = new Path("/foo");
      dfs.mkdirs(p);
      dfs.setPermission(p, new FsPermission((short)0700));
      bak = System.err;

      tmpUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          FsShell fshell = new FsShell(dfs.getConf());
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
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
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
      List<MaterializedReplica> replicas = getMaterializedReplicas(cluster);

      // Shut down miniCluster and then corrupt the block files by overwriting a
      // portion with junk data.  We must shut down the miniCluster so that threads
      // in the data node do not hold locks on the block files while we try to
      // write into them.  Particularly on Windows, the data node's use of the
      // FileChannel.transferTo method can cause block files to be memory mapped
      // in read-only mode during the transfer to a client, and this causes a
      // locking conflict.  The call to shutdown the miniCluster blocks until all
      // DataXceiver threads exit, preventing this problem.
      dfs.close();
      cluster.shutdown();

      show("replicas=" + replicas);
      corrupt(replicas, localfcontent);

      // Start the miniCluster again, but do not reformat, so prior files remain.
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

  /**
   * Test -stat [format] <path>... prints statistics about the file/directory
   * at <path> in the specified format.
   */
  @Test (timeout = 30000)
  public void testStat() throws Exception {
    final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    final Path testDir1 = new Path("testStat", "dir1");
    dfs.mkdirs(testDir1);
    final Path testFile2 = new Path(testDir1, "file2");
    DFSTestUtil.createFile(dfs, testFile2, 2 * BLOCK_SIZE, (short) 3, 0);
    final FileStatus status1 = dfs.getFileStatus(testDir1);
    final String mtime1 = fmt.format(new Date(status1.getModificationTime()));
    final FileStatus status2 = dfs.getFileStatus(testFile2);
    final String mtime2 = fmt.format(new Date(status2.getModificationTime()));

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));

    doFsStat(dfs.getConf(), null);

    out.reset();
    doFsStat(dfs.getConf(), null, testDir1);
    assertEquals("Unexpected -stat output: " + out,
        out.toString(), String.format("%s%n", mtime1));

    out.reset();
    doFsStat(dfs.getConf(), null, testDir1, testFile2);
    assertEquals("Unexpected -stat output: " + out,
        out.toString(), String.format("%s%n%s%n", mtime1, mtime2));

    doFsStat(dfs.getConf(), "%F %u:%g %b %y %n");
    out.reset();

    doFsStat(dfs.getConf(), "%F %a %A %u:%g %b %y %n", testDir1);
    assertTrue(out.toString(), out.toString().contains(mtime1));
    assertTrue(out.toString(), out.toString().contains("directory"));
    assertTrue(out.toString(), out.toString().contains(status1.getGroup()));
    assertTrue(out.toString(),
        out.toString().contains(status1.getPermission().toString()));

    int n = status1.getPermission().toShort();
    int octal = (n>>>9&1)*1000 + (n>>>6&7)*100 + (n>>>3&7)*10 + (n&7);
    assertTrue(out.toString(),
        out.toString().contains(String.valueOf(octal)));

    out.reset();
    doFsStat(dfs.getConf(), "%F %a %A %u:%g %b %y %n", testDir1, testFile2);

    n = status2.getPermission().toShort();
    octal = (n>>>9&1)*1000 + (n>>>6&7)*100 + (n>>>3&7)*10 + (n&7);
    assertTrue(out.toString(), out.toString().contains(mtime1));
    assertTrue(out.toString(), out.toString().contains("regular file"));
    assertTrue(out.toString(),
        out.toString().contains(status2.getPermission().toString()));
    assertTrue(out.toString(),
        out.toString().contains(String.valueOf(octal)));
    assertTrue(out.toString(), out.toString().contains(mtime2));
  }

  private static void doFsStat(Configuration conf, String format, Path... files)
      throws Exception {
    if (files == null || files.length == 0) {
      final String[] argv = (format == null ? new String[] {"-stat"} :
          new String[] {"-stat", format});
      assertEquals("Should have failed with missing arguments",
          -1, ToolRunner.run(new FsShell(conf), argv));
    } else {
      List<String> argv = new LinkedList<>();
      argv.add("-stat");
      if (format != null) {
        argv.add(format);
      }
      for (Path f : files) {
        argv.add(f.toString());
      }

      int ret = ToolRunner.run(new FsShell(conf), argv.toArray(new String[0]));
      assertEquals(argv + " returned non-zero status " + ret, 0, ret);
    }
  }

  @Test (timeout = 30000)
  public void testLsr() throws Exception {
    final Configuration conf = dfs.getConf();
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
      System.setOut(oldOut);
      System.setErr(oldErr);
      IOUtils.closeStream(out);
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
    FsShell shell = null;
    final String testdir = "/tmp/TestDFSShell-testCopyCommandsWithPreserveOption-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      dfs.mkdirs(hdfsTestDir);
      Path src = new Path(hdfsTestDir, "srcfile");
      dfs.create(src).close();

      dfs.setAcl(src, Lists.newArrayList(
          aclEntry(ACCESS, USER, ALL),
          aclEntry(ACCESS, USER, "foo", ALL),
          aclEntry(ACCESS, GROUP, READ_EXECUTE),
          aclEntry(ACCESS, GROUP, "bar", READ_EXECUTE),
          aclEntry(ACCESS, OTHER, EXECUTE)));

      FileStatus status = dfs.getFileStatus(src);
      final long mtime = status.getModificationTime();
      final long atime = status.getAccessTime();
      final String owner = status.getOwner();
      final String group = status.getGroup();
      final FsPermission perm = status.getPermission();

      dfs.setXAttr(src, USER_A1, USER_A1_VALUE);
      dfs.setXAttr(src, TRUSTED_A1, TRUSTED_A1_VALUE);

      shell = new FsShell(dfs.getConf());

      // -p
      Path target1 = new Path(hdfsTestDir, "targetfile1");
      String[] argv = new String[] { "-cp", "-p", src.toUri().toString(),
          target1.toUri().toString() };
      int ret = ToolRunner.run(shell, argv);
      assertEquals("cp -p is not working", SUCCESS, ret);
      FileStatus targetStatus = dfs.getFileStatus(target1);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      FsPermission targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      Map<String, byte[]> xattrs = dfs.getXAttrs(target1);
      assertTrue(xattrs.isEmpty());
      List<AclEntry> acls = dfs.getAclStatus(target1).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptop
      Path target2 = new Path(hdfsTestDir, "targetfile2");
      argv = new String[] { "-cp", "-ptop", src.toUri().toString(),
          target2.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptop is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(target2);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = dfs.getXAttrs(target2);
      assertTrue(xattrs.isEmpty());
      acls = dfs.getAclStatus(target2).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopx
      Path target3 = new Path(hdfsTestDir, "targetfile3");
      argv = new String[] { "-cp", "-ptopx", src.toUri().toString(),
          target3.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopx is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(target3);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = dfs.getXAttrs(target3);
      assertEquals(xattrs.size(), 2);
      assertArrayEquals(USER_A1_VALUE, xattrs.get(USER_A1));
      assertArrayEquals(TRUSTED_A1_VALUE, xattrs.get(TRUSTED_A1));
      acls = dfs.getAclStatus(target3).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopa
      Path target4 = new Path(hdfsTestDir, "targetfile4");
      argv = new String[] { "-cp", "-ptopa", src.toUri().toString(),
          target4.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopa is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(target4);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = dfs.getXAttrs(target4);
      assertTrue(xattrs.isEmpty());
      acls = dfs.getAclStatus(target4).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(dfs.getAclStatus(src), dfs.getAclStatus(target4));

      // -ptoa (verify -pa option will preserve permissions also)
      Path target5 = new Path(hdfsTestDir, "targetfile5");
      argv = new String[] { "-cp", "-ptoa", src.toUri().toString(),
          target5.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptoa is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(target5);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = dfs.getXAttrs(target5);
      assertTrue(xattrs.isEmpty());
      acls = dfs.getAclStatus(target5).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(dfs.getAclStatus(src), dfs.getAclStatus(target5));
    } finally {
      if (null != shell) {
        shell.close();
      }
    }
  }

  @Test (timeout = 120000)
  public void testCopyCommandsWithRawXAttrs() throws Exception {
    FsShell shell = null;
    final String testdir = "/tmp/TestDFSShell-testCopyCommandsWithRawXAttrs-"
      + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    final Path rawHdfsTestDir = new Path("/.reserved/raw" + testdir);
    try {
      dfs.mkdirs(hdfsTestDir);
      final Path src = new Path(hdfsTestDir, "srcfile");
      final String rawSrcBase = "/.reserved/raw" + testdir;
      final Path rawSrc = new Path(rawSrcBase, "srcfile");
      dfs.create(src).close();

      final Path srcDir = new Path(hdfsTestDir, "srcdir");
      final Path rawSrcDir = new Path("/.reserved/raw" + testdir, "srcdir");
      dfs.mkdirs(srcDir);
      final Path srcDirFile = new Path(srcDir, "srcfile");
      final Path rawSrcDirFile =
              new Path("/.reserved/raw" + srcDirFile);
      dfs.create(srcDirFile).close();

      final Path[] paths = { rawSrc, rawSrcDir, rawSrcDirFile };
      final String[] xattrNames = { USER_A1, RAW_A1 };
      final byte[][] xattrVals = { USER_A1_VALUE, RAW_A1_VALUE };

      for (int i = 0; i < paths.length; i++) {
        for (int j = 0; j < xattrNames.length; j++) {
          dfs.setXAttr(paths[i], xattrNames[j], xattrVals[j]);
        }
      }

      shell = new FsShell(dfs.getConf());

      /* Check that a file as the source path works ok. */
      doTestCopyCommandsWithRawXAttrs(shell, dfs, src, hdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, dfs, rawSrc, hdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, dfs, src, rawHdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, dfs, rawSrc, rawHdfsTestDir, true);

      /* Use a relative /.reserved/raw path. */
      final Path savedWd = dfs.getWorkingDirectory();
      try {
        dfs.setWorkingDirectory(new Path(rawSrcBase));
        final Path relRawSrc = new Path("../srcfile");
        final Path relRawHdfsTestDir = new Path("..");
        doTestCopyCommandsWithRawXAttrs(shell, dfs, relRawSrc,
            relRawHdfsTestDir, true);
      } finally {
        dfs.setWorkingDirectory(savedWd);
      }

      /* Check that a directory as the source path works ok. */
      doTestCopyCommandsWithRawXAttrs(shell, dfs, srcDir, hdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, dfs, rawSrcDir, hdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, dfs, srcDir, rawHdfsTestDir, false);
      doTestCopyCommandsWithRawXAttrs(shell, dfs, rawSrcDir, rawHdfsTestDir,
        true);

      /* Use relative in an absolute path. */
      final String relRawSrcDir = "./.reserved/../.reserved/raw/../raw" +
          testdir + "/srcdir";
      final String relRawDstDir = "./.reserved/../.reserved/raw/../raw" +
          testdir;
      doTestCopyCommandsWithRawXAttrs(shell, dfs, new Path(relRawSrcDir),
          new Path(relRawDstDir), true);
    } finally {
      if (null != shell) {
        shell.close();
      }
      dfs.delete(hdfsTestDir, true);
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
    FsShell shell = null;
    final String testdir =
        "/tmp/TestDFSShell-testCopyCommandsToDirectoryWithPreserveOption-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      dfs.mkdirs(hdfsTestDir);
      Path srcDir = new Path(hdfsTestDir, "srcDir");
      dfs.mkdirs(srcDir);

      dfs.setAcl(srcDir, Lists.newArrayList(
          aclEntry(ACCESS, USER, ALL),
          aclEntry(ACCESS, USER, "foo", ALL),
          aclEntry(ACCESS, GROUP, READ_EXECUTE),
          aclEntry(DEFAULT, GROUP, "bar", READ_EXECUTE),
          aclEntry(ACCESS, OTHER, EXECUTE)));
      // set sticky bit
      dfs.setPermission(srcDir,
          new FsPermission(ALL, READ_EXECUTE, EXECUTE, true));

      // Create a file in srcDir to check if modification time of
      // srcDir to be preserved after copying the file.
      // If cp -p command is to preserve modification time and then copy child
      // (srcFile), modification time will not be preserved.
      Path srcFile = new Path(srcDir, "srcFile");
      dfs.create(srcFile).close();

      FileStatus status = dfs.getFileStatus(srcDir);
      final long mtime = status.getModificationTime();
      final long atime = status.getAccessTime();
      final String owner = status.getOwner();
      final String group = status.getGroup();
      final FsPermission perm = status.getPermission();

      dfs.setXAttr(srcDir, USER_A1, USER_A1_VALUE);
      dfs.setXAttr(srcDir, TRUSTED_A1, TRUSTED_A1_VALUE);

      shell = new FsShell(dfs.getConf());

      // -p
      Path targetDir1 = new Path(hdfsTestDir, "targetDir1");
      String[] argv = new String[] { "-cp", "-p", srcDir.toUri().toString(),
          targetDir1.toUri().toString() };
      int ret = ToolRunner.run(shell, argv);
      assertEquals("cp -p is not working", SUCCESS, ret);
      FileStatus targetStatus = dfs.getFileStatus(targetDir1);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      FsPermission targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      Map<String, byte[]> xattrs = dfs.getXAttrs(targetDir1);
      assertTrue(xattrs.isEmpty());
      List<AclEntry> acls = dfs.getAclStatus(targetDir1).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptop
      Path targetDir2 = new Path(hdfsTestDir, "targetDir2");
      argv = new String[] { "-cp", "-ptop", srcDir.toUri().toString(),
          targetDir2.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptop is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(targetDir2);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = dfs.getXAttrs(targetDir2);
      assertTrue(xattrs.isEmpty());
      acls = dfs.getAclStatus(targetDir2).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopx
      Path targetDir3 = new Path(hdfsTestDir, "targetDir3");
      argv = new String[] { "-cp", "-ptopx", srcDir.toUri().toString(),
          targetDir3.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopx is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(targetDir3);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = dfs.getXAttrs(targetDir3);
      assertEquals(xattrs.size(), 2);
      assertArrayEquals(USER_A1_VALUE, xattrs.get(USER_A1));
      assertArrayEquals(TRUSTED_A1_VALUE, xattrs.get(TRUSTED_A1));
      acls = dfs.getAclStatus(targetDir3).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopa
      Path targetDir4 = new Path(hdfsTestDir, "targetDir4");
      argv = new String[] { "-cp", "-ptopa", srcDir.toUri().toString(),
          targetDir4.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopa is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(targetDir4);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = dfs.getXAttrs(targetDir4);
      assertTrue(xattrs.isEmpty());
      acls = dfs.getAclStatus(targetDir4).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(dfs.getAclStatus(srcDir), dfs.getAclStatus(targetDir4));

      // -ptoa (verify -pa option will preserve permissions also)
      Path targetDir5 = new Path(hdfsTestDir, "targetDir5");
      argv = new String[] { "-cp", "-ptoa", srcDir.toUri().toString(),
          targetDir5.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptoa is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(targetDir5);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      xattrs = dfs.getXAttrs(targetDir5);
      assertTrue(xattrs.isEmpty());
      acls = dfs.getAclStatus(targetDir5).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(dfs.getAclStatus(srcDir), dfs.getAclStatus(targetDir5));
    } finally {
      if (shell != null) {
        shell.close();
      }
    }
  }

  // Verify cp -pa option will preserve both ACL and sticky bit.
  @Test (timeout = 120000)
  public void testCopyCommandsPreserveAclAndStickyBit() throws Exception {
    FsShell shell = null;
    final String testdir =
        "/tmp/TestDFSShell-testCopyCommandsPreserveAclAndStickyBit-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      dfs.mkdirs(hdfsTestDir);
      Path src = new Path(hdfsTestDir, "srcfile");
      dfs.create(src).close();

      dfs.setAcl(src, Lists.newArrayList(
          aclEntry(ACCESS, USER, ALL),
          aclEntry(ACCESS, USER, "foo", ALL),
          aclEntry(ACCESS, GROUP, READ_EXECUTE),
          aclEntry(ACCESS, GROUP, "bar", READ_EXECUTE),
          aclEntry(ACCESS, OTHER, EXECUTE)));
      // set sticky bit
      dfs.setPermission(src,
          new FsPermission(ALL, READ_EXECUTE, EXECUTE, true));

      FileStatus status = dfs.getFileStatus(src);
      final long mtime = status.getModificationTime();
      final long atime = status.getAccessTime();
      final String owner = status.getOwner();
      final String group = status.getGroup();
      final FsPermission perm = status.getPermission();

      shell = new FsShell(dfs.getConf());

      // -p preserves sticky bit and doesn't preserve ACL
      Path target1 = new Path(hdfsTestDir, "targetfile1");
      String[] argv = new String[] { "-cp", "-p", src.toUri().toString(),
          target1.toUri().toString() };
      int ret = ToolRunner.run(shell, argv);
      assertEquals("cp is not working", SUCCESS, ret);
      FileStatus targetStatus = dfs.getFileStatus(target1);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      FsPermission targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      List<AclEntry> acls = dfs.getAclStatus(target1).getEntries();
      assertTrue(acls.isEmpty());
      assertFalse(targetPerm.getAclBit());

      // -ptopa preserves both sticky bit and ACL
      Path target2 = new Path(hdfsTestDir, "targetfile2");
      argv = new String[] { "-cp", "-ptopa", src.toUri().toString(),
          target2.toUri().toString() };
      ret = ToolRunner.run(shell, argv);
      assertEquals("cp -ptopa is not working", SUCCESS, ret);
      targetStatus = dfs.getFileStatus(target2);
      assertEquals(mtime, targetStatus.getModificationTime());
      assertEquals(atime, targetStatus.getAccessTime());
      assertEquals(owner, targetStatus.getOwner());
      assertEquals(group, targetStatus.getGroup());
      targetPerm = targetStatus.getPermission();
      assertTrue(perm.equals(targetPerm));
      acls = dfs.getAclStatus(target2).getEntries();
      assertFalse(acls.isEmpty());
      assertTrue(targetPerm.getAclBit());
      assertEquals(dfs.getAclStatus(src), dfs.getAclStatus(target2));
    } finally {
      if (null != shell) {
        shell.close();
      }
    }
  }

  // force Copy Option is -f
  @Test (timeout = 30000)
  public void testCopyCommandsWithForceOption() throws Exception {
    FsShell shell = null;
    final File localFile = new File(TEST_ROOT_DIR, "testFileForPut");
    final String localfilepath = new Path(localFile.getAbsolutePath()).toUri().toString();
    final String testdir = "/tmp/TestDFSShell-testCopyCommandsWithForceOption-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      dfs.mkdirs(hdfsTestDir);
      localFile.createNewFile();
      writeFile(dfs, new Path(testdir, "testFileForPut"));
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
    }
  }

  /* [refs HDFS-5033]
   *
   * return a "Permission Denied" message instead of "No such file or Directory"
   * when trying to put/copyFromLocal a file that doesn't have read access
   *
   */
  @Test (timeout = 30000)
  public void testCopyFromLocalWithPermissionDenied() throws Exception {
    FsShell shell = null;
    PrintStream bak = null;

    final File localFile = new File(TEST_ROOT_DIR, "testFileWithNoReadPermissions");
    final String localfilepath = new Path(localFile.getAbsolutePath()).toUri().toString();
    final String testdir = "/tmp/TestDFSShell-CopyFromLocalWithPermissionDenied-"
        + counter.getAndIncrement();
    final Path hdfsTestDir = new Path(testdir);
    try {
      dfs.mkdirs(hdfsTestDir);
      localFile.createNewFile();
      localFile.setReadable(false);
      writeFile(dfs, new Path(testdir, "testFileForPut"));
      shell = new FsShell();

      // capture system error messages, snarfed from testErrOutPut()
      bak = System.err;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      PrintStream tmp = new PrintStream(out);
      System.setErr(tmp);

      // Tests for put
      String[] argv = new String[] { "-put", localfilepath, testdir };
      int res = ToolRunner.run(shell, argv);
      assertEquals("put is working", ERROR, res);
      String returned = out.toString();
      assertTrue(" outputs Permission denied error message",
          (returned.lastIndexOf("Permission denied") != -1));

      // Tests for copyFromLocal
      argv = new String[] { "-copyFromLocal", localfilepath, testdir };
      res = ToolRunner.run(shell, argv);
      assertEquals("copyFromLocal -f is working", ERROR, res);
      returned = out.toString();
      assertTrue(" outputs Permission denied error message",
          (returned.lastIndexOf("Permission denied") != -1));

    } finally {
      if (bak != null) {
        System.setErr(bak);
      }

      if (null != shell)
        shell.close();

      if (localFile.exists())
        localFile.delete();

      dfs.delete(hdfsTestDir, true);
    }
  }

  /**
   * Test -setrep with a replication factor that is too low.  We have to test
   * this here because the mini-miniCluster used with testHDFSConf.xml uses a
   * replication factor of 1 (for good reason).
   */
  @Test (timeout = 30000)
  public void testSetrepLow() throws Exception {
    Configuration conf = new Configuration();

    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 2);

    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    MiniDFSCluster cluster = builder.numDataNodes(2).format(true).build();
    FsShell shell = new FsShell(conf);

    cluster.waitActive();

    final String testdir = "/tmp/TestDFSShell-testSetrepLow";
    final Path hdfsFile = new Path(testdir, "testFileForSetrepLow");
    final PrintStream origOut = System.out;
    final PrintStream origErr = System.err;

    try {
      final FileSystem fs = cluster.getFileSystem();

      assertTrue("Unable to create test directory",
          fs.mkdirs(new Path(testdir)));

      fs.create(hdfsFile, true).close();

      // Capture the command output so we can examine it
      final ByteArrayOutputStream bao = new ByteArrayOutputStream();
      final PrintStream capture = new PrintStream(bao);

      System.setOut(capture);
      System.setErr(capture);

      final String[] argv = new String[] { "-setrep", "1", hdfsFile.toString() };

      try {
        assertEquals("Command did not return the expected exit code",
            1, shell.run(argv));
      } finally {
        System.setOut(origOut);
        System.setErr(origErr);
      }

      assertEquals("Error message is not the expected error message",
          "setrep: Requested replication factor of 1 is less than "
              + "the required minimum of 2 for /tmp/TestDFSShell-"
              + "testSetrepLow/testFileForSetrepLow\n",
          bao.toString());
    } finally {
      shell.close();
      cluster.shutdown();
    }
  }

  // setrep for file and directory.
  @Test (timeout = 30000)
  public void testSetrep() throws Exception {
    FsShell shell = null;
    final String testdir1 = "/tmp/TestDFSShell-testSetrep-" + counter.getAndIncrement();
    final String testdir2 = testdir1 + "/nestedDir";
    final Path hdfsFile1 = new Path(testdir1, "testFileForSetrep");
    final Path hdfsFile2 = new Path(testdir2, "testFileForSetrep");
    final Short oldRepFactor = new Short((short) 2);
    final Short newRepFactor = new Short((short) 3);
    try {
      String[] argv;
      assertThat(dfs.mkdirs(new Path(testdir2)), is(true));
      shell = new FsShell(dfs.getConf());

      dfs.create(hdfsFile1, true).close();
      dfs.create(hdfsFile2, true).close();

      // Tests for setrep on a file.
      argv = new String[] { "-setrep", newRepFactor.toString(), hdfsFile1.toString() };
      assertThat(shell.run(argv), is(SUCCESS));
      assertThat(dfs.getFileStatus(hdfsFile1).getReplication(), is(newRepFactor));
      assertThat(dfs.getFileStatus(hdfsFile2).getReplication(), is(oldRepFactor));

      // Tests for setrep

      // Tests for setrep on a directory and make sure it is applied recursively.
      argv = new String[] { "-setrep", newRepFactor.toString(), testdir1 };
      assertThat(shell.run(argv), is(SUCCESS));
      assertThat(dfs.getFileStatus(hdfsFile1).getReplication(), is(newRepFactor));
      assertThat(dfs.getFileStatus(hdfsFile2).getReplication(), is(newRepFactor));

    } finally {
      if (shell != null) {
        shell.close();
      }
    }
  }

  /**
   * Delete a file optionally configuring trash on the server and client.
   */
  private void deleteFileUsingTrash(
      boolean serverTrash, boolean clientTrash) throws Exception {
    // Run a miniCluster, optionally with trash enabled on the server
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

    // Run appendToFile with insufficient arguments.
    FsShell shell = new FsShell();
    shell.setConf(dfs.getConf());
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
  }

  @Test (timeout = 30000)
  public void testSetXAttrPermission() throws Exception {
    UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] {"mygroup"});
    PrintStream bak = null;
    try {
      Path p = new Path("/foo");
      dfs.mkdirs(p);
      bak = System.err;

      final FsShell fshell = new FsShell(dfs.getConf());
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setErr(new PrintStream(out));

      // No permission to write xattr
      dfs.setPermission(p, new FsPermission((short) 0700));
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
      dfs.setPermission(p, new FsPermission((short) 0750));
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
    }
  }

  /* HDFS-6413 xattr names erroneously handled as case-insensitive */
  @Test (timeout = 30000)
  public void testSetXAttrCaseSensitivity() throws Exception {
    PrintStream bak = null;
    try {
      Path p = new Path("/mydir");
      dfs.mkdirs(p);
      bak = System.err;

      final FsShell fshell = new FsShell(dfs.getConf());
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
    final String root = "/testSetXAttrPermissionAsDifferentOwner";
    final String USER1 = "user1";
    final String GROUP1 = "supergroup";
    final UserGroupInformation user1 = UserGroupInformation.
        createUserForTesting(USER1, new String[] {GROUP1});
    final UserGroupInformation user2 = UserGroupInformation.
        createUserForTesting("user2", new String[] {"mygroup2"});
    final UserGroupInformation SUPERUSER = UserGroupInformation.getCurrentUser();
    PrintStream bak = null;
    try {
      dfs.mkdirs(new Path(root));
      dfs.setOwner(new Path(root), USER1, GROUP1);
      bak = System.err;

      final FsShell fshell = new FsShell(dfs.getConf());
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setErr(new PrintStream(out));

      //Test 1.  Let user1 be owner for /foo
      user1.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final int ret = ToolRunner.run(fshell, new String[]{
              "-mkdir", root + "/foo"});
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
              "-chmod", "707", root + "/foo"});
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
              "-setfattr", "-n", "user.a1", "-v", "1234", root + "/foo"});
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
              "-setfattr", "-n", "user.a1", "-v", "1234", root + "/foo"});
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
              "user.a1", root + "/foo" });
          assertEquals("Returned should be 0", 0, ret);
          out.reset();
          // Remove
          ret = ToolRunner.run(fshell, new String[] { "-setfattr", "-x",
              "user.a1", root + "/foo" });
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
              "-chmod", "700", root + "/foo"});
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
              "user.a2", root + "/foo" });
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
              "user.a2", root + "/foo" });
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
              "trusted.a3", root + "/foo" });
          assertEquals("Returned should be 0", 0, ret);
          out.reset();
          return null;
        }
      });
    } finally {
      if (bak != null) {
        System.setErr(bak);
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
    PrintStream bakErr = null;
    try {
      final Path p = new Path("/testGetFAttrErrors");
      dfs.mkdirs(p);
      bakErr = System.err;

      final FsShell fshell = new FsShell(dfs.getConf());
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setErr(new PrintStream(out));

      // No permission for "other".
      dfs.setPermission(p, new FsPermission((short) 0700));

      {
        final int ret = ToolRunner.run(fshell, new String[] {
            "-setfattr", "-n", "user.a1", "-v", "1234", p.toString()});
        assertEquals("Returned should be 0", 0, ret);
        out.reset();
      }

      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            int ret = ToolRunner.run(fshell, new String[] {
                "-getfattr", "-n", "user.a1", p.toString()});
            String str = out.toString();
            assertTrue("xattr value was incorrectly returned",
                str.indexOf("1234") == -1);
            out.reset();
            return null;
          }
        });

      {
        final int ret = ToolRunner.run(fshell, new String[]{
            "-getfattr", "-n", "user.nonexistent", p.toString()});
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

  @Test (timeout = 30000)
  public void testListReserved() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    FileSystem fs = cluster.getFileSystem();
    FsShell shell = new FsShell();
    shell.setConf(conf);
    FileStatus test = fs.getFileStatus(new Path("/.reserved"));
    assertEquals(FSDirectory.DOT_RESERVED_STRING, test.getPath().getName());

    // Listing /.reserved/ should show 2 items: raw and .inodes
    FileStatus[] stats = fs.listStatus(new Path("/.reserved"));
    assertEquals(2, stats.length);
    assertEquals(FSDirectory.DOT_INODES_STRING, stats[0].getPath().getName());
    assertEquals(conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY),
        stats[0].getGroup());
    assertEquals("raw", stats[1].getPath().getName());
    assertEquals(conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY),
        stats[1].getGroup());

    // Listing / should not show /.reserved
    stats = fs.listStatus(new Path("/"));
    assertEquals(0, stats.length);

    // runCmd prints error into System.err, thus verify from there.
    PrintStream syserr = System.err;
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    System.setErr(ps);
    try {
      runCmd(shell, "-ls", "/.reserved");
      assertEquals(0, baos.toString().length());

      runCmd(shell, "-ls", "/.reserved/raw/.reserved");
      assertTrue(baos.toString().contains("No such file or directory"));
    } finally {
      System.setErr(syserr);
      cluster.shutdown();
    }
  }

  @Test (timeout = 30000)
  public void testMkdirReserved() throws IOException {
    try {
      dfs.mkdirs(new Path("/.reserved"));
      fail("Can't mkdir /.reserved");
    } catch (Exception e) {
      // Expected, HadoopIllegalArgumentException thrown from remote
      assertTrue(e.getMessage().contains("\".reserved\" is reserved"));
    }
  }

  @Test (timeout = 30000)
  public void testRmReserved() throws IOException {
    try {
      dfs.delete(new Path("/.reserved"), true);
      fail("Can't delete /.reserved");
    } catch (Exception e) {
      // Expected, InvalidPathException thrown from remote
      assertTrue(e.getMessage().contains("Invalid path name /.reserved"));
    }
  }

  @Test //(timeout = 30000)
  public void testCopyReserved() throws IOException {
    final File localFile = new File(TEST_ROOT_DIR, "testFileForPut");
    localFile.createNewFile();
    final String localfilepath =
        new Path(localFile.getAbsolutePath()).toUri().toString();
    try {
      dfs.copyFromLocalFile(new Path(localfilepath), new Path("/.reserved"));
      fail("Can't copyFromLocal to /.reserved");
    } catch (Exception e) {
      // Expected, InvalidPathException thrown from remote
      assertTrue(e.getMessage().contains("Invalid path name /.reserved"));
    }

    final String testdir = GenericTestUtils.getTempPath(
        "TestDFSShell-testCopyReserved");
    final Path hdfsTestDir = new Path(testdir);
    writeFile(dfs, new Path(testdir, "testFileForPut"));
    final Path src = new Path(hdfsTestDir, "srcfile");
    dfs.create(src).close();
    assertTrue(dfs.exists(src));

    // runCmd prints error into System.err, thus verify from there.
    PrintStream syserr = System.err;
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    System.setErr(ps);
    try {
      FsShell shell = new FsShell(dfs.getConf());
      runCmd(shell, "-cp", src.toString(), "/.reserved");
      assertTrue(baos.toString().contains("Invalid path name /.reserved"));
    } finally {
      System.setErr(syserr);
    }
  }

  @Test (timeout = 30000)
  public void testChmodReserved() throws IOException {
    // runCmd prints error into System.err, thus verify from there.
    PrintStream syserr = System.err;
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    System.setErr(ps);
    try {
      FsShell shell = new FsShell(dfs.getConf());
      runCmd(shell, "-chmod", "777", "/.reserved");
      assertTrue(baos.toString().contains("Invalid path name /.reserved"));
    } finally {
      System.setErr(syserr);
    }
  }

  @Test (timeout = 30000)
  public void testChownReserved() throws IOException {
    // runCmd prints error into System.err, thus verify from there.
    PrintStream syserr = System.err;
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    System.setErr(ps);
    try {
      FsShell shell = new FsShell(dfs.getConf());
      runCmd(shell, "-chown", "user1", "/.reserved");
      assertTrue(baos.toString().contains("Invalid path name /.reserved"));
    } finally {
      System.setErr(syserr);
    }
  }

  @Test (timeout = 30000)
  public void testSymLinkReserved() throws IOException {
    try {
      dfs.createSymlink(new Path("/.reserved"), new Path("/rl1"), false);
      fail("Can't create symlink to /.reserved");
    } catch (Exception e) {
      // Expected, InvalidPathException thrown from remote
      assertTrue(e.getMessage().contains("Invalid target name: /.reserved"));
    }
  }

  @Test (timeout = 30000)
  public void testSnapshotReserved() throws IOException {
    final Path reserved = new Path("/.reserved");
    try {
      dfs.allowSnapshot(reserved);
      fail("Can't allow snapshot on /.reserved");
    } catch (FileNotFoundException e) {
      assertTrue(e.getMessage().contains("Directory does not exist"));
    }
    try {
      dfs.createSnapshot(reserved, "snap");
      fail("Can't create snapshot on /.reserved");
    } catch (FileNotFoundException e) {
      assertTrue(e.getMessage().contains("Directory/File does not exist"));
    }
  }
}
