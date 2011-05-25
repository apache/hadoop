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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

/**
 * This class tests commands from Trash.
 */
public class TestTrash extends TestCase {

  private final static Path TEST_DIR =
    new Path(new File(System.getProperty("test.build.data","/tmp")
          ).toURI().toString().replace(' ', '+'), "testTrash");

  protected static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    out.writeBytes("dhruba: " + f);
    out.close();
    assertTrue(fs.exists(f));
    return f;
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
    Path p = new Path(trashRoot+"/"+ path.toUri().getPath());
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
   * 
   * Test trash for the shell's delete command for the default file system
   * specified in the paramter conf
   * @param conf 
   * @param base - the base path where files are created
   * @param trashRoot - the expected place where the trashbin resides
   * @throws IOException
   */
  public static void trashShell(final Configuration conf, final Path base,
      FileSystem trashRootFs, Path trashRoot)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);

    conf.set(FS_TRASH_INTERVAL_KEY, "0"); // disabled
    assertFalse(new Trash(conf).isEnabled());

    conf.set(FS_TRASH_INTERVAL_KEY, "10"); // 10 minute
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
    writeFile(fs, myFile);

    // Verify that expunge without Trash directory
    // won't throw Exception
    {
      String[] args = new String[1];
      args[0] = "-expunge";
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // Verify that we succeed in removing the file we created.
    // This should go into Trash.
    {
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

 
      checkTrash(trashRootFs, trashRoot, fs.makeQualified(myFile));
    }

    // Verify that we can recreate the file
    writeFile(fs, myFile);

    // Verify that we succeed in removing the file we re-created
    {
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = new Path(base, "test/mkdirs/myFile").toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // Verify that we can recreate the file
    writeFile(fs, myFile);
    
    // Verify that we succeed in removing the whole directory
    // along with the file inside it.
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = new Path(base, "test/mkdirs").toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // recreate directory
    mkdir(fs, myPath);

    // Verify that we succeed in removing the whole directory
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = new Path(base, "test/mkdirs").toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // Check that we can delete a file from the trash
    {
        Path toErase = new Path(trashRoot, "toErase");
        int retVal = -1;
        writeFile(trashRootFs, toErase);
        try {
          retVal = shell.run(new String[] {"-rm", toErase.toString()});
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(retVal == 0);
        checkNotInTrash (trashRootFs, trashRoot, toErase.toString());
        checkNotInTrash (trashRootFs, trashRoot, toErase.toString()+".1");
    }

    // simulate Trash removal
    {
      String[] args = new String[1];
      args[0] = "-expunge";
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // verify that after expunging the Trash, it really goes away
    checkNotInTrash(trashRootFs, trashRoot, new Path(base, "test/mkdirs/myFile").toString());

    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile);

    // remove file first, then remove directory
    {
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
      checkTrash(trashRootFs, trashRoot, myFile);

      args = new String[2];
      args[0] = "-rmr";
      args[1] = myPath.toString();
      val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
      checkTrash(trashRootFs, trashRoot, myPath);
    }

    // attempt to remove parent of trash
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = trashRoot.getParent().getParent().toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertEquals("exit code", 1, val);
      assertTrue(trashRootFs.exists(trashRoot));
    }
    
    // Verify skip trash option really works
    
    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile);
    
    // Verify that skip trash option really skips the trash for files (rm)
    {
      String[] args = new String[3];
      args[0] = "-rm";
      args[1] = "-skipTrash";
      args[2] = myFile.toString();
      int val = -1;
      try {
        // Clear out trash
        assertEquals(0, shell.run(new String [] { "-expunge" } ));
        
        val = shell.run(args);
        
      }catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
            e.getLocalizedMessage());
      }
      assertFalse("Expected TrashRoot (" + trashRoot + 
          ") to exist in file system:"
          + trashRootFs.getUri(), 
          trashRootFs.exists(trashRoot)); // No new Current should be created
      assertFalse(fs.exists(myFile));
      assertTrue(val == 0);
    }
    
    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile);
    
    // Verify that skip trash option really skips the trash for rmr
    {
      String[] args = new String[3];
      args[0] = "-rmr";
      args[1] = "-skipTrash";
      args[2] = myPath.toString();

      int val = -1;
      try {
        // Clear out trash
        assertEquals(0, shell.run(new String [] { "-expunge" } ));
        
        val = shell.run(args);
        
      }catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
            e.getLocalizedMessage());
      }

      assertFalse(trashRootFs.exists(trashRoot)); // No new Current should be created
      assertFalse(fs.exists(myPath));
      assertFalse(fs.exists(myFile));
      assertTrue(val == 0);
    }
    
    // deleting same file multiple times
    {     
      int val = -1;
      mkdir(fs, myPath);
      
      try {
        assertEquals(0, shell.run(new String [] { "-expunge" } ));
      } catch (Exception e) {
        System.err.println("Exception raised from fs expunge " +
            e.getLocalizedMessage());        
      }
      
      // create a file in that directory.
      myFile = new Path(base, "test/mkdirs/myFile");
      String [] args = new String[] {"-rm", myFile.toString()};
      int num_runs = 10;
      for(int i=0;i<num_runs; i++) {
        
        //create file
        writeFile(fs, myFile);
         
        // delete file
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
              e.getLocalizedMessage());
        }
        assertTrue(val==0);
      }
      // current trash directory
      Path trashDir = new Path(trashRoot.toUri().getPath() + myFile.getParent().toUri().getPath());
      
      System.out.println("Deleting same myFile: myFile.parent=" + myFile.getParent().toUri().getPath() + 
          "; trashroot="+trashRoot.toUri().getPath() + 
          "; trashDir=" + trashDir.toUri().getPath());
      
      int count = countSameDeletedFiles(fs, trashDir, myFile);
      System.out.println("counted " + count + " files " + myFile.getName() + "* in " + trashDir);
      assertTrue(count==num_runs);
    }
    
  }

  public static void trashNonDefaultFS(Configuration conf) throws IOException {
    conf.set(FS_TRASH_INTERVAL_KEY, "10"); // 10 minute
    // attempt non-default FileSystem trash
    {
      final FileSystem lfs = FileSystem.getLocal(conf);
      Path p = TEST_DIR;
      Path f = new Path(p, "foo/bar");
      if (lfs.exists(p)) {
        lfs.delete(p, true);
      }
      try {
        f = writeFile(lfs, f);

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

  public void testTrash() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    trashShell(conf, TEST_DIR, null, null);
  }

  public void testNonDefaultFS() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    conf.set("fs.defaultFS", "invalid://host/bar/foo");
    trashNonDefaultFS(conf);
  }
  
  public void testTrashEmptier() throws Exception {
    Configuration conf = new Configuration();
    // Trash with 12 second deletes and 6 seconds checkpoints
    conf.set(FS_TRASH_INTERVAL_KEY, "0.2"); // 12 seconds
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    conf.set(FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // 6 seconds
    Trash trash = new Trash(conf);

    // Start Emptier in background
    Runnable emptier = trash.getEmptier();
    Thread emptierThread = new Thread(emptier);
    emptierThread.start();

    FileSystem fs = FileSystem.getLocal(conf);
    conf.set("fs.defaultFS", fs.getUri().toString());
    FsShell shell = new FsShell();
    shell.setConf(conf);
    shell.init();
    // First create a new directory with mkdirs
    Path myPath = new Path(TEST_DIR, "test/mkdirs");
    mkdir(fs, myPath);
    int fileIndex = 0;
    Set<String> checkpoints = new HashSet<String>();
    while (true)  {
      // Create a file with a new name
      Path myFile = new Path(TEST_DIR, "test/mkdirs/myFile" + fileIndex++);
      writeFile(fs, myFile);

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
   * @see TestCase#tearDown()
   */
  @Override
  protected void tearDown() throws IOException {
    File trashDir = new File(TEST_DIR.toUri().getPath());
    if (trashDir.exists() && !FileUtil.fullyDelete(trashDir)) {
      throw new IOException("Cannot remove data directory: " + trashDir);
    }
  }

  static class TestLFS extends LocalFileSystem {
    Path home;
    TestLFS() {
      this(new Path(TEST_DIR, "user/test"));
    }
    TestLFS(Path home) {
      super();
      this.home = home;
    }
    public Path getHomeDirectory() {
      return home;
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
    conf.set(FS_TRASH_INTERVAL_KEY, "10"); //minutes..
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
      
      writeFile(fs, myFile);
      
      start = System.currentTimeMillis();
      
      try {
        retVal = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
            e.getLocalizedMessage());
        throw new IOException(e.getMessage());
      }
      
      assertTrue(retVal == 0);
      
      long iterTime = System.currentTimeMillis() - start;
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
  
  public static void main(String [] arg) throws IOException{
    // run performance piece as a separate test
    performanceTestDeleteSameFile();
  }
}
