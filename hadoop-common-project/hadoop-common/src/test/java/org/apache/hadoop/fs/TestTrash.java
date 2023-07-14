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


import static org.apache.hadoop.fs.CommonConfigurationKeys.*;
import static org.apache.hadoop.fs.FileSystemTestHelper.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.TrashPolicyDefault.Emptier;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;

/**
 * This class tests commands from Trash.
 */
public class TestTrash {

  private final static File BASE_PATH = new File(GenericTestUtils.getTempPath(
      "testTrash"));

  private final static Path TEST_DIR = new Path(BASE_PATH.getAbsolutePath());

  @Before
  public void setUp() throws IOException {
    // ensure each test initiates a FileSystem instance,
    // avoid getting an old instance from cache.
    FileSystem.closeAll();
  }

  protected static Path mkdir(FileSystem fs, Path p) throws IOException {
    assertTrue(fs.mkdirs(p));
    assertTrue(fs.exists(p));
    assertTrue(fs.getFileStatus(p).isDirectory());
    return p;
  }

  // check that the specified file is in Trash
  protected static void checkTrash(FileSystem trashFs, Path trashRoot,
      Path path) throws IOException {
    Path p = Path.mergePaths(trashRoot, path);
    assertTrue("Could not find file in trash: "+ p , trashFs.exists(p));
  }
  
  // counts how many instances of the file are in the Trash
  // they all are in format fileName*
  protected static int countSameDeletedFiles(FileSystem fs, 
      Path trashDir, Path fileName) throws IOException {

    final String prefix = fileName.getName();
    System.out.println("Counting " + fileName + " in " + trashDir.toString());

    // filter that matches all the files that start with fileName*
    PathFilter pf = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        return file.getName().startsWith(prefix);
      }
    };
    // run the filter
    FileStatus [] fss = fs.listStatus(trashDir, pf);

    return fss==null? 0 : fss.length;
  }

  // check that the specified file is not in Trash
  static void checkNotInTrash(FileSystem fs, Path trashRoot, String pathname)
                              throws IOException {
    Path p = new Path(trashRoot+"/"+ new Path(pathname).getName());
    assertTrue(!fs.exists(p));
  }
  
  /**
   * Test trash for the shell's delete command for the file system fs
   * @param fs
   * @param base - the base path where files are created
   * @throws IOException
   */
  public static void trashShell(final FileSystem fs, final Path base)
      throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", fs.getUri().toString());
    trashShell(conf, base, null, null);
  }

  /**
   * Test trash for the shell's delete command for the default file system
   * specified in the parameter conf
   * @param conf - configuration object for the filesystem
   * @param base - the base path where files are created
   * @param trashRootFs - the filesystem object to test trash
   * @param trashRoot - the expected place where the trashbin resides
   * @throws IOException
   */
  public static void trashShell(final Configuration conf, final Path base,
      FileSystem trashRootFs, Path trashRoot) throws Exception {
    FileSystem fs = FileSystem.get(conf);

    conf.setLong(FS_TRASH_INTERVAL_KEY, 0); // disabled
    assertFalse(new Trash(conf).isEnabled());

    conf.setLong(FS_TRASH_INTERVAL_KEY, -1); // disabled
    assertFalse(new Trash(conf).isEnabled());

    conf.setLong(FS_TRASH_INTERVAL_KEY, 10); // 10 minute
    assertTrue(new Trash(conf).isEnabled());

    FsShell shell = new FsShell();
    shell.setConf(conf);
    if (trashRoot == null) {
      trashRoot = shell.getCurrentTrashDir();
    }
    if (trashRootFs == null) {
      trashRootFs = fs;
    }

    // First create a new directory with mkdirs
    Path myPath = new Path(base, "test/mkdirs");
    mkdir(fs, myPath);

    // Second, create a file in that directory.
    Path myFile = new Path(base, "test/mkdirs/myFile");
    writeFile(fs, myFile, 10);

    // Verify that expunge without Trash directory
    // won't throw Exception
    {
      String[] args = new String[1];
      args[0] = "-expunge";
      int val = -1;
      val = shell.run(args);

      assertEquals("Expunge should return zero", 0, val);
    }

    // Verify that we succeed in removing the file we created.
    // This should go into Trash.
    {
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = myFile.toString();
      int val = -1;
      val = shell.run(args);

      assertEquals("Remove should return zero", 0, val);

      checkTrash(trashRootFs, trashRoot, fs.makeQualified(myFile));
    }

    // Verify that we can recreate the file
    writeFile(fs, myFile, 10);

    // Verify that we succeed in removing the file we re-created
    {
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = new Path(base, "test/mkdirs/myFile").toString();
      int val = -1;
      val = shell.run(args);

      assertEquals("Remove should return zero", 0, val);
    }

    // Verify that we can recreate the file
    writeFile(fs, myFile, 10);
    
    // Verify that we succeed in removing the whole directory
    // along with the file inside it.
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = new Path(base, "test/mkdirs").toString();
      int val = -1;
      val = shell.run(args);

      assertEquals("Recursive Remove should return zero", 0, val);
    }

    // recreate directory
    mkdir(fs, myPath);

    // Verify that we succeed in removing the whole directory
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = new Path(base, "test/mkdirs").toString();
      int val = -1;
      val = shell.run(args);

      assertEquals("Recursive Remove should return zero", 0, val);
    }

    // Check that we can delete a file from the trash
    {
      Path toErase = new Path(trashRoot, "toErase");
      int val = -1;
      writeFile(trashRootFs, toErase, 10);

      val = shell.run(new String[] {"-rm", toErase.toString()});

      assertEquals("Recursive Remove should return zero", 0, val);
      checkNotInTrash(trashRootFs, trashRoot, toErase.toString());
      checkNotInTrash(trashRootFs, trashRoot, toErase.toString()+".1");
    }

    // simulate Trash removal
    {
      String[] args = new String[1];
      args[0] = "-expunge";
      int val = -1;
      val = shell.run(args);

      assertEquals("Expunge should return zero", 0, val);
    }

    // verify that after expunging the Trash, it really goes away
    checkNotInTrash(trashRootFs, trashRoot, new Path(
        base, "test/mkdirs/myFile").toString());

    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile, 10);

    // remove file first, then remove directory
    {
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = myFile.toString();
      int val = -1;
      val = shell.run(args);

      assertEquals("Remove should return zero", 0, val);
      checkTrash(trashRootFs, trashRoot, myFile);

      args = new String[2];
      args[0] = "-rmr";
      args[1] = myPath.toString();
      val = -1;
      val = shell.run(args);

      assertEquals("Recursive Remove should return zero", 0, val);
      checkTrash(trashRootFs, trashRoot, myPath);
    }

    // attempt to remove parent of trash
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = trashRoot.getParent().getParent().toString();
      int val = -1;
      val = shell.run(args);

      assertEquals("Recursive Remove should return exit code 1", 1, val);
      assertTrue(trashRootFs.exists(trashRoot));
    }
    
    // Verify skip trash option really works
    
    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile, 10);
    
    // Verify that skip trash option really skips the trash for files (rm)
    {
      String[] args = new String[3];
      args[0] = "-rm";
      args[1] = "-skipTrash";
      args[2] = myFile.toString();
      int val = -1;
      // Clear out trash
      assertEquals("-expunge failed",
          0, shell.run(new String[] {"-expunge" }));

      val = shell.run(args);

      assertFalse("Expected TrashRoot (" + trashRoot + 
          ") to exist in file system:"
          + trashRootFs.getUri(), 
          trashRootFs.exists(trashRoot)); // No new Current should be created
      assertFalse(fs.exists(myFile));
      assertEquals("Remove with skipTrash should return zero", 0, val);
    }
    
    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile, 10);
    
    // Verify that skip trash option really skips the trash for rmr
    {
      String[] args = new String[3];
      args[0] = "-rmr";
      args[1] = "-skipTrash";
      args[2] = myPath.toString();

      int val = -1;
      // Clear out trash
      assertEquals(0, shell.run(new String[] {"-expunge" }));
        
      val = shell.run(args);

      assertFalse(trashRootFs.exists(trashRoot)); // No new Current should be created
      assertFalse(fs.exists(myPath));
      assertFalse(fs.exists(myFile));
      assertEquals("Remove with skipTrash should return zero", 0, val);
    }
    
    // deleting same file multiple times
    {     
      int val = -1;
      mkdir(fs, myPath);
      assertEquals("Expunge should return zero",
            0, shell.run(new String[] {"-expunge" }));

      
      // create a file in that directory.
      myFile = new Path(base, "test/mkdirs/myFile");
      String[] args = new String[] {"-rm", myFile.toString()};
      int num_runs = 10;
      for(int i=0; i<num_runs; i++) {
        
        //create file
        writeFile(fs, myFile, 10);
         
        // delete file
        val = shell.run(args);

        assertEquals("Remove should return zero", 0, val);
      }
      // current trash directory
      Path trashDir = Path.mergePaths(new Path(trashRoot.toUri().getPath()),
          new Path(myFile.getParent().toUri().getPath()));
      
      System.out.println("Deleting same myFile: myFile.parent="
          + myFile.getParent().toUri().getPath() +
          "; trashroot="+trashRoot.toUri().getPath() + 
          "; trashDir=" + trashDir.toUri().getPath());
      
      int count = countSameDeletedFiles(fs, trashDir, myFile);
      System.out.println("counted " + count + " files "
          + myFile.getName() + "* in " + trashDir);
      assertEquals("Count should have returned 10", num_runs, count);
    }
    
    //Verify skipTrash option is suggested when rm fails due to its absence
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = "/";  //This always contains trash directory
      PrintStream stdout = System.out;
      PrintStream stderr = System.err;
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      PrintStream newOut = new PrintStream(byteStream);
      System.setOut(newOut);
      System.setErr(newOut);

      shell.run(args);

      String output = byteStream.toString();
      System.setOut(stdout);
      System.setErr(stderr);
      assertTrue("skipTrash wasn't suggested as remedy to failed rm command" +
          " or we deleted / even though we could not get server defaults",
          output.indexOf("Consider using -skipTrash option") != -1 ||
          output.indexOf("Failed to determine server "
              + "trash configuration") != -1);
    }

    // Verify old checkpoint format is recognized
    {
      // emulate two old trash checkpoint directories, one that is old enough
      // to be deleted on the next expunge and one that isn't.
      long trashInterval = conf.getLong(FS_TRASH_INTERVAL_KEY,
          FS_TRASH_INTERVAL_DEFAULT);
      long now = Time.now();
      DateFormat oldCheckpointFormat = new SimpleDateFormat("yyMMddHHmm");
      Path dirToDelete = new Path(trashRoot.getParent(),
          oldCheckpointFormat.format(now - (trashInterval * 60 * 1000) - 1));
      Path dirToKeep = new Path(trashRoot.getParent(),
          oldCheckpointFormat.format(now));
      mkdir(trashRootFs, dirToDelete);
      mkdir(trashRootFs, dirToKeep);

      // Clear out trash
      int rc = -1;
      rc = shell.run(new String[] {"-expunge" });

      assertEquals("Expunge should return zero", 0, rc);
      assertFalse("old checkpoint format not recognized",
          trashRootFs.exists(dirToDelete));
      assertTrue("old checkpoint format directory should not be removed",
          trashRootFs.exists(dirToKeep));
    }

    // Verify expunge -immediate removes all checkpoints and current folder
    {
      // Setup a recent and old checkpoint and a current folder
      // to be deleted on the next expunge and one that isn't.
      long trashInterval = conf.getLong(FS_TRASH_INTERVAL_KEY,
          FS_TRASH_INTERVAL_DEFAULT);
      long now = Time.now();
      DateFormat checkpointFormat = new SimpleDateFormat("yyMMddHHmm");
      Path oldCheckpoint = new Path(trashRoot.getParent(),
          checkpointFormat.format(now - (trashInterval * 60 * 1000) - 1));
      Path recentCheckpoint = new Path(trashRoot.getParent(),
          checkpointFormat.format(now));
      Path currentFolder = new Path(trashRoot.getParent(), "Current");
      mkdir(trashRootFs, oldCheckpoint);
      mkdir(trashRootFs, recentCheckpoint);
      mkdir(trashRootFs, currentFolder);

      // Clear out trash
      int rc = -1;
      rc = shell.run(new String[] {"-expunge", "-immediate"});

      assertEquals("Expunge immediate should return zero", 0, rc);
      assertFalse("Old checkpoint should be removed",
          trashRootFs.exists(oldCheckpoint));
      assertFalse("Recent checkpoint should be removed",
          trashRootFs.exists(recentCheckpoint));
      assertFalse("Current folder should be removed",
          trashRootFs.exists(currentFolder));
      assertEquals("Ensure trash folder is empty", 0,
          trashRootFs.listStatus(trashRoot.getParent()).length);
    }
  }

  @Test
  public void testExpungeWithFileSystem() throws Exception {
    {
      Configuration config = new Configuration();

      config.setClass("fs.testlfs.impl", TestLFS.class, FileSystem.class);
      TestLFS testlfs = new TestLFS();
      testlfs.setUri("testlfs:/");
      URI testlfsURI = testlfs.getUri();
      config.set("fs.defaultFS", testlfsURI.toString());

      config.setLong(FS_TRASH_INTERVAL_KEY, 10); // 10 minute
      assertTrue(new Trash(config).isEnabled());


      FileSystem.addFileSystemForTesting(testlfsURI, config, testlfs);
      testlfs.initialize(testlfsURI, config);

      FsShell testlfsshell = new FsShell();
      testlfsshell.setConf(config);

      Path trashRoot = testlfsshell.getCurrentTrashDir();

      long trashInterval = config.getLong(FS_TRASH_INTERVAL_KEY,
          FS_TRASH_INTERVAL_DEFAULT);
      long now = Time.now();
      DateFormat checkpointFormat = new SimpleDateFormat("yyMMddHHmm");
      Path oldCheckpoint = new Path(trashRoot.getParent(),
          checkpointFormat.format(now - (trashInterval * 60 * 1000) - 1));
      Path recentCheckpoint = new Path(trashRoot.getParent(),
          checkpointFormat.format(now));
      Path currentFolder = new Path(trashRoot.getParent(), "Current");

      Path myPath = new Path(TEST_DIR, "test/mkdirs");

      Path myFile = new Path(TEST_DIR, "test/mkdirs/testFile");

      mkdir(testlfs, oldCheckpoint);
      mkdir(testlfs, recentCheckpoint);
      mkdir(testlfs, currentFolder);
      mkdir(testlfs, myPath);
      writeFile(testlfs, myFile, 10);

      String[] args = new String[]{"-expunge", "-immediate",
          "-fs", "testlfs:/"};
      int val = testlfsshell.run(args);

      assertEquals("Expunge immediate with filesystem should return zero",
          0, val);
      assertFalse("Old checkpoint should be removed",
          testlfs.exists(oldCheckpoint));
      assertFalse("Recent checkpoint should be removed",
          testlfs.exists(recentCheckpoint));
      assertFalse("Current folder should be removed",
          testlfs.exists(currentFolder));
      assertEquals("Ensure trash folder is empty", 0,
          testlfs.listStatus(trashRoot.getParent()).length);

      // Incorrect FileSystem scheme
      String incorrectFS = "incorrectfs:/";
      args = new String[]{"-expunge", "-immediate",
          "-fs", incorrectFS};
      val = testlfsshell.run(args);

      assertEquals("Expunge immediate should return exit code 1 when "
              + "incorrect Filesystem is passed",
          1, val);

      // Empty FileSystem scheme
      args = new String[]{"-expunge", "-immediate",
          "-fs", ""};
      val = testlfsshell.run(args);

      assertNotEquals("Expunge immediate should fail when filesystem is NULL",
          0, val);
      FileSystem.removeFileSystemForTesting(testlfsURI, config, testlfs);
    }
  }

  public static void trashNonDefaultFS(Configuration conf) throws IOException {
    conf.setLong(FS_TRASH_INTERVAL_KEY, 10); // 10 minute
    // attempt non-default FileSystem trash
    {
      final FileSystem lfs = FileSystem.getLocal(conf);
      Path p = TEST_DIR;
      Path f = new Path(p, "foo/bar");
      if (lfs.exists(p)) {
        lfs.delete(p, true);
      }
      try {
        writeFile(lfs, f, 10);
        FileSystem.closeAll();
        FileSystem localFs = FileSystem.get(URI.create("file:///"), conf);
        Trash lTrash = new Trash(localFs, conf);
        lTrash.moveToTrash(f.getParent());
        checkTrash(localFs, lTrash.getCurrentTrashDir(), f);
      } finally {
        if (lfs.exists(p)) {
          lfs.delete(p, true);
        }
      }
    }
  }

  @Test
  public void testTrash() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    trashShell(FileSystem.getLocal(conf), TEST_DIR);
  }

  @Test
  public void testExistingFileTrash() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    FileSystem fs = FileSystem.getLocal(conf);
    conf.set("fs.defaultFS", fs.getUri().toString());
    conf.setLong(FS_TRASH_INTERVAL_KEY, 0); // disabled
    assertFalse(new Trash(conf).isEnabled());

    conf.setLong(FS_TRASH_INTERVAL_KEY, -1); // disabled
    assertFalse(new Trash(conf).isEnabled());

    conf.setLong(FS_TRASH_INTERVAL_KEY, 10); // 10 minute
    assertTrue(new Trash(conf).isEnabled());

    FsShell shell = new FsShell();
    shell.setConf(conf);

    // First create a new directory with mkdirs
    Path myPath = new Path(TEST_DIR, "test/mkdirs");
    mkdir(fs, myPath);

    // Second, create a file in that directory.
    Path myFile = new Path(TEST_DIR, "test/mkdirs/myExistingFile");
    writeFile(fs, myFile, 10);
    // First rm a file
    mkdir(fs, myPath);
    writeFile(fs, myFile, 10);

    String[] args1 = new String[2];
    args1[0] = "-rm";
    args1[1] = myFile.toString();
    int val1 = -1;
    try {
      val1 = shell.run(args1);
    } catch (Exception e) {
      System.err.println("Exception raised from Trash.run " +
          e.getLocalizedMessage());
    }
    assertTrue(val1 == 0);

    // Second  rm a file which parent path is the same as above
    mkdir(fs, myFile);
    writeFile(fs, new Path(myFile, "mySubFile"), 10);
    String[] args2 = new String[2];
    args2[0] = "-rm";
    args2[1] = new Path(myFile, "mySubFile").toString();
    int val2 = -1;
    try {
      val2 = shell.run(args2);
    } catch (Exception e) {
      System.err.println("Exception raised from Trash.run " +
          e.getLocalizedMessage());
    }
    assertTrue(val2 == 0);
  }

  @Test
  public void testNonDefaultFS() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    conf.set("fs.defaultFS", "invalid://host/bar/foo");
    trashNonDefaultFS(conf);
  }

  @Test
  public void testPluggableTrash() throws IOException {
    Configuration conf = new Configuration();

    // Test plugged TrashPolicy
    conf.setClass("fs.trash.classname", TestTrashPolicy.class, TrashPolicy.class);
    Trash trash = new Trash(conf);
    assertTrue(trash.getTrashPolicy().getClass().equals(TestTrashPolicy.class));
  }

  @Test
  public void testCheckpointInterval() throws IOException {
    // Verify if fs.trash.checkpoint.interval is set to positive number
    // but bigger than fs.trash.interval,
    // the value should be reset to fs.trash.interval
    verifyDefaultPolicyIntervalValues(10, 12, 10);

    // Verify if fs.trash.checkpoint.interval is set to positive number
    // and smaller than fs.trash.interval, the value should be respected
    verifyDefaultPolicyIntervalValues(10, 5, 5);

    // Verify if fs.trash.checkpoint.interval sets to 0
    // the value should be reset to fs.trash.interval
    verifyDefaultPolicyIntervalValues(10, 0, 10);

    // Verify if fs.trash.checkpoint.interval sets to a negative number
    // the value should be reset to fs.trash.interval
    verifyDefaultPolicyIntervalValues(10, -1, 10);
  }

  @Test
  public void testMoveEmptyDirToTrash() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(FS_FILE_IMPL_KEY,
        RawLocalFileSystem.class,
        FileSystem.class);
    conf.setLong(FS_TRASH_INTERVAL_KEY, 1); // 1 min
    FileSystem fs = FileSystem.get(conf);
    verifyMoveEmptyDirToTrash(fs, conf);
  }

  /**
   * Simulate the carrier process of the trash emptier restarts,
   * verify it honors the <b>fs.trash.interval</b> before and after restart.
   * @throws Exception
   */
  @Test
  public void testTrashRestarts() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.trash.classname",
        AuditableTrashPolicy.class,
        TrashPolicy.class);
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    conf.set(FS_TRASH_INTERVAL_KEY, "50"); // in milliseconds for test
    Trash trash = new Trash(conf);
    // create 5 checkpoints
    for(int i=0; i<5; i++) {
      trash.checkpoint();
    }

    // Run the trash emptier for 120ms, it should run
    // 2 times deletion as the interval is 50ms.
    // Verify the checkpoints number when shutting down the emptier.
    verifyAuditableTrashEmptier(trash, 120, 3);

    // reconfigure the interval to 100 ms
    conf.set(FS_TRASH_INTERVAL_KEY, "100");
    Trash trashNew = new Trash(conf);

    // Run the trash emptier for 120ms, it should run
    // 1 time deletion.
    verifyAuditableTrashEmptier(trashNew, 120, 2);
  }

  @Test
  public void testTrashPermission()  throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.trash.classname",
        TrashPolicyDefault.class,
        TrashPolicy.class);
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    conf.set(FS_TRASH_INTERVAL_KEY, "0.2");
    verifyTrashPermission(FileSystem.getLocal(conf), conf);
  }

  @Test
  public void testTrashEmptier() throws Exception {
    Configuration conf = new Configuration();
    // Trash with 12 second deletes and 6 seconds checkpoints
    conf.set(FS_TRASH_INTERVAL_KEY, "0.2"); // 12 seconds
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    conf.set(FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // 6 seconds
    FileSystem fs = FileSystem.getLocal(conf);
    conf.set("fs.default.name", fs.getUri().toString());
    
    Trash trash = new Trash(conf);

    // Start Emptier in background
    Runnable emptier = trash.getEmptier();
    Thread emptierThread = new Thread(emptier);
    emptierThread.start();

    FsShell shell = new FsShell();
    shell.setConf(conf);
    shell.init();
    // First create a new directory with mkdirs
    Path myPath = new Path(TEST_DIR, "test/mkdirs");
    mkdir(fs, myPath);
    int fileIndex = 0;
    Set<String> checkpoints = new HashSet<>();
    while (true)  {
      // Create a file with a new name
      Path myFile = new Path(TEST_DIR, "test/mkdirs/myFile" + fileIndex++);
      writeFile(fs, myFile, 10);

      // Delete the file to trash
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = myFile.toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);

      Path trashDir = shell.getCurrentTrashDir();
      FileStatus files[] = fs.listStatus(trashDir.getParent());
      // Scan files in .Trash and add them to set of checkpoints
      for (FileStatus file : files) {
        String fileName = file.getPath().getName();
        checkpoints.add(fileName);
      }
      // If checkpoints has 4 objects it is Current + 3 checkpoint directories
      if (checkpoints.size() == 4) {
        // The actual contents should be smaller since the last checkpoint
        // should've been deleted and Current might not have been recreated yet
        assertTrue(checkpoints.size() > files.length);
        break;
      }
      Thread.sleep(5000);
    }
    emptierThread.interrupt();
    emptierThread.join();
  }

  /**
   * Test trash emptier can delete non-checkpoint dir or not.
   * @throws Exception
   */
  @Test()
  public void testTrashEmptierCleanDirNotInCheckpointDir() throws Exception {
    Configuration conf = new Configuration();
    // Trash with 12 second deletes and 6 seconds checkpoints.
    conf.set(FS_TRASH_INTERVAL_KEY, "0.2"); // 12 seconds
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    conf.set(FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // 6 seconds
    conf.setBoolean(FS_TRASH_CLEAN_TRASHROOT_ENABLE_KEY, true);
    FileSystem fs = FileSystem.getLocal(conf);
    conf.set("fs.default.name", fs.getUri().toString());

    Trash trash = new Trash(conf);

    // Start Emptier in background.
    Runnable emptier = trash.getEmptier();
    Thread emptierThread = new Thread(emptier);
    emptierThread.start();

    FsShell shell = new FsShell();
    shell.setConf(conf);
    shell.init();

    // Make sure the .Trash dir existed.
    mkdir(fs, shell.getCurrentTrashDir());
    assertTrue(fs.exists(shell.getCurrentTrashDir()));
    // Create a directory under .Trash directly.
    Path myPath = new Path(shell.getCurrentTrashDir().getParent(), "test_dirs");
    mkdir(fs, myPath);
    assertTrue(fs.exists(myPath));

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return !fs.exists(myPath);
        } catch (IOException e) {
          // Do nothing.
        }
        return false;
      }
    }, 6000, 60000);
    emptierThread.interrupt();
    emptierThread.join();
  }

  @After
  public void tearDown() throws IOException {
    File trashDir = new File(TEST_DIR.toUri().getPath());
    if (trashDir.exists() && !FileUtil.fullyDelete(trashDir)) {
      throw new IOException("Cannot remove data directory: " + trashDir);
    }
  }

  public static class TestLFS extends LocalFileSystem {
    private URI uriName = null;
    Path home;
    TestLFS() {
      this(TEST_DIR);
    }
    TestLFS(final Path home) {
      super(new RawLocalFileSystem() {
        @Override
        protected Path getInitialWorkingDirectory() {
          return makeQualified(home);
        }

        @Override
        public Path getHomeDirectory() {
          return makeQualified(home);
        }
      });
      this.home = home;
    }
    @Override
    public Path getHomeDirectory() {
      return home;
    }

    @Override
    public URI getUri() {
      if(uriName == null){
        return super.getUri();
      } else {
        return uriName;
      }
    }

    @Override
    public String getScheme() {
      return "testlfs";
    }

    public void setUri(String uri){
      uriName = URI.create(uri);
    }
  }
  
  /**
   *  test same file deletion - multiple time
   *  this is more of a performance test - shouldn't be run as a unit test
   * @throws IOException
   */
  public static void performanceTestDeleteSameFile() throws IOException{
    Path base = TEST_DIR;
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    FileSystem fs = FileSystem.getLocal(conf);
    
    conf.set("fs.defaultFS", fs.getUri().toString());
    conf.setLong(FS_TRASH_INTERVAL_KEY, 10); //minutes..
    FsShell shell = new FsShell();
    shell.setConf(conf);
    //Path trashRoot = null;

    Path myPath = new Path(base, "test/mkdirs");
    mkdir(fs, myPath);

    // create a file in that directory.
    Path myFile;
    long start;
    long first = 0;
    int retVal = 0;
    int factor = 10; // how much slower any of subsequent deletion can be
    myFile = new Path(base, "test/mkdirs/myFile");
    String [] args = new String[] {"-rm", myFile.toString()};
    int iters = 1000;
    for(int i=0;i<iters; i++) {
      
      writeFile(fs, myFile, 10);
      
      start = Time.now();
      
      try {
        retVal = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
            e.getLocalizedMessage());
        throw new IOException(e.getMessage());
      }
      
      assertTrue(retVal == 0);
      
      long iterTime = Time.now() - start;
      // take median of the first 10 runs
      if(i<10) {
        if(i==0) {
          first = iterTime;
        }
        else {
          first = (first + iterTime)/2;
        }
      }
      // we don't want to print every iteration - let's do every 10th
      int print_freq = iters/10; 
      
      if(i>10) {
        if((i%print_freq) == 0)
          System.out.println("iteration="+i+";res =" + retVal + "; start=" + start
              + "; iterTime = " + iterTime + " vs. firstTime=" + first);
        long factoredTime = first*factor;
        assertTrue(iterTime<factoredTime); //no more then twice of median first 10
      }
    }
  }

  public static void verifyMoveEmptyDirToTrash(FileSystem fs,
      Configuration conf) throws IOException {
    Path caseRoot = new Path(
        GenericTestUtils.getTempPath("testUserTrash"));
    Path testRoot = new Path(caseRoot, "trash-users");
    Path emptyDir = new Path(testRoot, "empty-dir");
    try (FileSystem fileSystem = fs){
      fileSystem.mkdirs(emptyDir);
      Trash trash = new Trash(fileSystem, conf);
      // Make sure trash root is clean
      Path trashRoot = trash.getCurrentTrashDir(emptyDir);
      fileSystem.delete(trashRoot, true);
      // Move to trash should be succeed
      assertTrue("Move an empty directory to trash failed",
          trash.moveToTrash(emptyDir));
      // Verify the empty dir is removed
      assertFalse("The empty directory still exists on file system",
          fileSystem.exists(emptyDir));
      emptyDir = fileSystem.makeQualified(emptyDir);
      Path dirInTrash = Path.mergePaths(trashRoot, emptyDir);
      assertTrue("Directory wasn't moved to trash",
          fileSystem.exists(dirInTrash));
      FileStatus[] flist = fileSystem.listStatus(dirInTrash);
      assertTrue("Directory is not empty",
          flist!= null && flist.length == 0);
    }
  }

  /**
   * Create a bunch of files and set with different permission, after
   * moved to trash, verify the location in trash directory is expected
   * and the permission is reserved.
   *
   * @throws IOException
   */
  public static void verifyTrashPermission(FileSystem fs, Configuration conf)
      throws IOException {
    Path caseRoot = new Path(BASE_PATH.getPath(),
        "testTrashPermission");
    try (FileSystem fileSystem = fs){
      Trash trash = new Trash(fileSystem, conf);
      FileSystemTestWrapper wrapper =
          new FileSystemTestWrapper(fileSystem);

      short[] filePermssions = {
          (short) 0600,
          (short) 0644,
          (short) 0660,
          (short) 0700,
          (short) 0750,
          (short) 0755,
          (short) 0775,
          (short) 0777
      };

      for(int i=0; i<filePermssions.length; i++) {
        // Set different permission to files
        FsPermission fsPermission = new FsPermission(filePermssions[i]);
        Path file = new Path(caseRoot, "file" + i);
        byte[] randomBytes = new byte[new Random().nextInt(10)];
        wrapper.writeFile(file, randomBytes);
        wrapper.setPermission(file, fsPermission);

        // Move file to trash
        trash.moveToTrash(file);

        // Verify the file is moved to trash, at expected location
        Path trashDir = trash.getCurrentTrashDir(file);
        if(!file.isAbsolute()) {
          file = wrapper.makeQualified(file);
        }
        Path fileInTrash = Path.mergePaths(trashDir, file);
        FileStatus fstat = wrapper.getFileStatus(fileInTrash);
        assertTrue(String.format("File %s is not moved to trash",
            fileInTrash.toString()),
            wrapper.exists(fileInTrash));
        // Verify permission not change
        assertTrue(String.format("Expected file: %s is %s, but actual is %s",
            fileInTrash.toString(),
            fsPermission.toString(),
            fstat.getPermission().toString()),
            fstat.getPermission().equals(fsPermission));
      }

      // Verify the trash directory can be removed
      Path trashRoot = trash.getCurrentTrashDir();
      assertTrue(wrapper.delete(trashRoot, true));
    }
  }

  private void verifyDefaultPolicyIntervalValues(long trashInterval,
      long checkpointInterval, long expectedInterval) throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(FS_TRASH_INTERVAL_KEY, trashInterval);
    conf.set("fs.trash.classname", TrashPolicyDefault.class.getName());
    conf.setLong(FS_TRASH_CHECKPOINT_INTERVAL_KEY, checkpointInterval);
    Trash trash = new Trash(conf);
    Emptier emptier = (Emptier)trash.getEmptier();
    assertEquals(expectedInterval, emptier.getEmptierInterval());
  }

  /**
   * Launch the {@link Trash} emptier for given milliseconds,
   * verify the number of checkpoints is expected.
   */
  private void verifyAuditableTrashEmptier(Trash trash,
      long timeAlive,
      int expectedNumOfCheckpoints)
          throws IOException {
    Thread emptierThread = null;
    try {
      Runnable emptier = trash.getEmptier();
      emptierThread = new Thread(emptier);
      emptierThread.start();

      // Shutdown the emptier thread after a given time
      Thread.sleep(timeAlive);
      emptierThread.interrupt();
      emptierThread.join();

      AuditableTrashPolicy at = (AuditableTrashPolicy) trash.getTrashPolicy();
      assertEquals(
          String.format("Expected num of checkpoints is %s, but actual is %s",
              expectedNumOfCheckpoints, at.getNumberOfCheckpoints()),
          expectedNumOfCheckpoints,
          at.getNumberOfCheckpoints());
    } catch (InterruptedException  e) {
      // Ignore
    } finally {
      // Avoid thread leak
      if(emptierThread != null) {
        emptierThread.interrupt();
      }
    }
  }

  // Test TrashPolicy. Don't care about implementation.
  public static class TestTrashPolicy extends TrashPolicy {
    public TestTrashPolicy() { }

    @Override
    public void initialize(Configuration conf, FileSystem fs, Path home) {
    }

    @Override
    public void initialize(Configuration conf, FileSystem fs) {
    }

    @Override
    public boolean isEnabled() {
      return false;
    }

    @Override 
    public boolean moveToTrash(Path path) throws IOException {
      return false;
    }

    @Override
    public void createCheckpoint() throws IOException {
    }

    @Override
    public void deleteCheckpoint() throws IOException {
    }

    @Override
    public void deleteCheckpointsImmediately() throws IOException {
    }

    @Override
    public Path getCurrentTrashDir() {
      return null;
    }

    @Override
    public Path getCurrentTrashDir(Path path) throws IOException {
      return null;
    }

    @Override
    public Runnable getEmptier() throws IOException {
      return null;
    }
  }

  /**
   * A fake {@link TrashPolicy} implementation, it keeps a count
   * on number of checkpoints in the trash. It doesn't do anything
   * other than updating the count.
   *
   */
  public static class AuditableTrashPolicy extends TrashPolicy {

    public AuditableTrashPolicy() {}

    public AuditableTrashPolicy(Configuration conf)
        throws IOException {
      this.initialize(conf, null);
    }

    @Override
    @Deprecated
    public void initialize(Configuration conf, FileSystem fs, Path home) {
      this.deletionInterval = (long)(conf.getFloat(
          FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT));
    }

    @Override
    public void initialize(Configuration conf, FileSystem fs) {
      this.deletionInterval = (long)(conf.getFloat(
          FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT));
    }

    @Override
    public boolean moveToTrash(Path path) throws IOException {
      return false;
    }

    @Override
    public void createCheckpoint() throws IOException {
      AuditableCheckpoints.add();
    }

    @Override
    public void deleteCheckpoint() throws IOException {
      AuditableCheckpoints.delete();
    }

    @Override
    public void deleteCheckpointsImmediately() throws IOException {
      AuditableCheckpoints.deleteAll();
    }

    @Override
    public Path getCurrentTrashDir() {
      return null;
    }

    @Override
    public Runnable getEmptier() throws IOException {
      return new AuditableEmptier(getConf());
    }

    public int getNumberOfCheckpoints() {
      return AuditableCheckpoints.get();
    }

    /**
     * A fake emptier that simulates to delete a checkpoint
     * in a fixed interval.
     */
    private class AuditableEmptier implements Runnable {
      private Configuration conf = null;
      public AuditableEmptier(Configuration conf) {
        this.conf = conf;
      }

      @Override
      public void run() {
        AuditableTrashPolicy trash = null;
        try {
          trash = new AuditableTrashPolicy(conf);
        } catch (IOException e1) {}
        while(true) {
          try {
            Thread.sleep(deletionInterval);
            trash.deleteCheckpoint();
          } catch (IOException e) {
            // no exception
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    }

    @Override
    public boolean isEnabled() {
      return true;
    }
  }

  /**
   * Only counts the number of checkpoints, not do anything more.
   * Declared as an inner static class to share state between
   * testing threads.
   */
  private static class AuditableCheckpoints {

    private static final Logger LOG =
        LoggerFactory.getLogger(AuditableCheckpoints.class);

    private static AtomicInteger numOfCheckpoint =
        new AtomicInteger(0);

    private static void add() {
      numOfCheckpoint.incrementAndGet();
      LOG.info("Create a checkpoint, current number of checkpoints {}",
          numOfCheckpoint.get());
    }

    private static void delete() {
      if(numOfCheckpoint.get() > 0) {
        numOfCheckpoint.decrementAndGet();
        LOG.info("Delete a checkpoint, current number of checkpoints {}",
            numOfCheckpoint.get());
      }
    }

    private static void deleteAll() {
      numOfCheckpoint.set(0);
      LOG.info("Delete all checkpoints, current number of checkpoints {}",
          numOfCheckpoint.get());
    }

    private static int get() {
      return numOfCheckpoint.get();
    }
  }
}
