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
import java.net.URI;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;


/**
 * A JUnit test for copying files recursively.
 */
public class TestCopyFiles extends TestCase {
  
  static final URI LOCAL_FS = URI.create("file:///");
  
  private static final int NFILES = 20;
  private static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');

  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private static class MyFile {
    private static Random gen = new Random();
    private static final int MAX_LEVELS = 3;
    private static final int MAX_SIZE = 8*1024;
    private static String[] dirNames = {
      "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
    };
    private final String name;
    private int size = 0;
    private long seed = 0L;

    MyFile() {
      this(gen.nextInt(MAX_LEVELS));
    }
    MyFile(int nLevels) {
      String xname = "";
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        xname = sb.toString();
      }
      long fidx = gen.nextLong() & Long.MAX_VALUE;
      name = xname + Long.toString(fidx);
      reset();
    }
    void reset() {
      final int oldsize = size;
      do { size = gen.nextInt(MAX_SIZE); } while (oldsize == size);
      final long oldseed = seed;
      do { seed = gen.nextLong() & Long.MAX_VALUE; } while (oldseed == seed);
    }
    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }

  /** create NFILES with random names and directory hierarchies
   * with random (but reproducible) data in them.
   */
  private static MyFile[] createFiles(URI fsname, String topdir)
    throws IOException {
    FileSystem fs = FileSystem.get(fsname, new Configuration());
    Path root = new Path(topdir);

    MyFile[] files = new MyFile[NFILES];
    for (int i = 0; i < NFILES; i++) {
      files[i] = createFile(root, fs);
    }
    return files;
  }

  static MyFile createFile(Path root, FileSystem fs, int levels)
      throws IOException {
    MyFile f = levels < 0 ? new MyFile() : new MyFile(levels);
    Path p = new Path(root, f.getName());
    FSDataOutputStream out = fs.create(p);
    byte[] toWrite = new byte[f.getSize()];
    new Random(f.getSeed()).nextBytes(toWrite);
    out.write(toWrite);
    out.close();
    FileSystem.LOG.info("created: " + p + ", size=" + f.getSize());
    return f;
  }

  static MyFile createFile(Path root, FileSystem fs) throws IOException {
    return createFile(root, fs, -1);
  }

  /** check if the files have been copied correctly. */
  private static boolean checkFiles(String fsname, String topdir, MyFile[] files) 
    throws IOException {
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getNamed(fsname, conf);
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < files.length; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      FSDataInputStream in = fs.open(fPath);
      byte[] toRead = new byte[files[idx].getSize()];
      byte[] toCompare = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toCompare);
      assertEquals("Cannnot read file.", toRead.length, in.read(toRead));
      in.close();
      for (int i = 0; i < toRead.length; i++) {
        if (toRead[i] != toCompare[i]) {
          return false;
        }
      }
      toRead = null;
      toCompare = null;
    }
    
    return true;
  }

  private static void updateFiles(String fsname, String topdir, MyFile[] files,
        int nupdate) throws IOException {
    assert nupdate <= NFILES;

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getNamed(fsname, conf);
    Path root = new Path(topdir);

    for (int idx = 0; idx < nupdate; ++idx) {
      Path fPath = new Path(root, files[idx].getName());
      // overwrite file
      assertTrue(fPath.toString() + " does not exist", fs.exists(fPath));
      FSDataOutputStream out = fs.create(fPath);
      files[idx].reset();
      byte[] toWrite = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toWrite);
      out.write(toWrite);
      out.close();
    }
  }

  private static FileStatus[] getFileStatus(String namenode,
      String topdir, MyFile[] files) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getNamed(namenode, conf);
    Path root = new Path(topdir);
    FileStatus[] ret = new FileStatus[NFILES];
    for (int idx = 0; idx < NFILES; ++idx) {
      ret[idx] = fs.getFileStatus(new Path(root, files[idx].getName()));
    }
    return ret;
  }

  private static boolean checkUpdate(FileStatus[] old, String namenode,
      String topdir, MyFile[] upd, final int nupdate) throws IOException {

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getNamed(namenode, conf);
    Path root = new Path(topdir);

    // overwrote updated files
    for (int idx = 0; idx < nupdate; ++idx) {
      final FileStatus stat =
        fs.getFileStatus(new Path(root, upd[idx].getName()));
      if (stat.getModificationTime() <= old[idx].getModificationTime()) {
        return false;
      }
    }
    // did not overwrite files not updated
    for (int idx = nupdate; idx < NFILES; ++idx) {
      final FileStatus stat =
        fs.getFileStatus(new Path(root, upd[idx].getName()));
      if (stat.getModificationTime() != old[idx].getModificationTime()) {
        return false;
      }
    }
    return true;
  }

  /** delete directory and everything underneath it.*/
  private static void deldir(String fsname, String topdir)
    throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getNamed(fsname, conf);
    Path root = new Path(topdir);
    fs.delete(root, true);
  }
  
  /** copy files from local file system to local file system */
  public void testCopyFromLocalToLocal() throws Exception {
    MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR+"/srcdat");
    ToolRunner.run(new DistCp(new Configuration()),
                           new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                                         "file:///"+TEST_ROOT_DIR+"/destdat"});
    assertTrue("Source and destination directories do not match.",
               checkFiles("file:///", TEST_ROOT_DIR+"/destdat", files));
    deldir("file:///", TEST_ROOT_DIR+"/destdat");
    deldir("file:///", TEST_ROOT_DIR+"/srcdat");
  }
  
  /** copy files from dfs file system to dfs file system */
  public void testCopyFromDfsToDfs() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(namenode, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                   fs.exists(new Path(namenode+"/logs")));
        deldir(namenode, "/destdat");
        deldir(namenode, "/srcdat");
        deldir(namenode, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /** copy files from local file system to dfs file system */
  public void testCopyFromLocalToDfs() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 1, true, null);
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR+"/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         namenode+"/logs",
                                         "file:///"+TEST_ROOT_DIR+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(namenode, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                    fs.exists(new Path(namenode+"/logs")));
        deldir(namenode, "/destdat");
        deldir(namenode, "/logs");
        deldir("file:///", TEST_ROOT_DIR+"/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** copy files from dfs file system to local file system */
  public void testCopyFromDfsToLocal() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 1, true, null);
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         "/logs",
                                         namenode+"/srcdat",
                                         "file:///"+TEST_ROOT_DIR+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles("file:///", TEST_ROOT_DIR+"/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                    fs.exists(new Path("/logs")));
        deldir("file:///", TEST_ROOT_DIR+"/destdat");
        deldir(namenode, "/logs");
        deldir(namenode, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testCopyDfsToDfsUpdateOverwrite() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-p",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(namenode, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                    fs.exists(new Path(namenode+"/logs")));

        FileStatus[] dchkpoint = getFileStatus(namenode, "/destdat", files);
        final int nupdate = NFILES>>2;
        updateFiles(namenode, "/srcdat", files, nupdate);
        deldir(namenode, "/logs");

        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-p",
                                         "-update",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(namenode, "/destdat", files));
        assertTrue("Update failed to replicate all changes in src",
                 checkUpdate(dchkpoint, namenode, "/destdat", files, nupdate));

        deldir(namenode, "/logs");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-p",
                                         "-overwrite",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(namenode, "/destdat", files));
        assertTrue("-overwrite didn't.",
                 checkUpdate(dchkpoint, namenode, "/destdat", files, NFILES));

        deldir(namenode, "/destdat");
        deldir(namenode, "/srcdat");
        deldir(namenode, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testCopyDuplication() throws Exception {
    try {    
      MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR+"/srcdat");
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/src2/srcdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles("file:///", TEST_ROOT_DIR+"/src2/srcdat", files));
  
      assertEquals(DistCp.DuplicationException.ERROR_CODE,
          ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/src2/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/destdat",}));
    }
    finally {
      deldir("file:///", TEST_ROOT_DIR+"/destdat");
      deldir("file:///", TEST_ROOT_DIR+"/srcdat");
      deldir("file:///", TEST_ROOT_DIR+"/src2");
    }
  }

  public void testCopySingleFile() throws Exception {
    FileSystem fs = FileSystem.get(LOCAL_FS, new Configuration());
    Path root = new Path(TEST_ROOT_DIR+"/srcdat");
    try {    
      MyFile[] files = {createFile(root, fs)};
      //copy a dir with a single file
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/destdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles("file:///", TEST_ROOT_DIR+"/destdat", files));
      
      //copy a single file
      String fname = files[0].getName();
      Path p = new Path(root, fname);
      FileSystem.LOG.info("fname=" + fname + ", exists? " + fs.exists(p));
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat/"+fname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"+fname});
      assertTrue("Source and destination directories do not match.",
          checkFiles("file:///", TEST_ROOT_DIR+"/dest2", files));     
      //copy single file to existing dir
      deldir("file:///", TEST_ROOT_DIR+"/dest2");
      fs.mkdirs(new Path(TEST_ROOT_DIR+"/dest2"));
      MyFile[] files2 = {createFile(root, fs, 0)};
      String sname = files2[0].getName();
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"-update",
                        "file:///"+TEST_ROOT_DIR+"/srcdat/"+sname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"});
      assertTrue("Source and destination directories do not match.",
          checkFiles("file:///", TEST_ROOT_DIR+"/dest2", files2));     
      updateFiles("file:///", TEST_ROOT_DIR+"/srcdat", files2, 1);
      //copy single file to existing dir w/ dst name conflict
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"-update",
                        "file:///"+TEST_ROOT_DIR+"/srcdat/"+sname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"});
      assertTrue("Source and destination directories do not match.",
          checkFiles("file:///", TEST_ROOT_DIR+"/dest2", files2));     
    }
    finally {
      deldir("file:///", TEST_ROOT_DIR+"/destdat");
      deldir("file:///", TEST_ROOT_DIR+"/dest2");
      deldir("file:///", TEST_ROOT_DIR+"/srcdat");
    }
  }

  public void testPreserveOption() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      {//test preserving user
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(nnUri, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), "u" + i, null);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pu", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(nnUri, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(nnUri, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "u" + i, dststat[i].getOwner());
        }
        deldir(nnUri, "/destdat");
        deldir(nnUri, "/srcdat");
      }

      {//test preserving group
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(nnUri, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), null, "g" + i);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pg", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(nnUri, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(nnUri, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "g" + i, dststat[i].getGroup());
        }
        deldir(nnUri, "/destdat");
        deldir(nnUri, "/srcdat");
      }

      {//test preserving mode
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(nnUri, "/srcdat", files);
        FsPermission[] permissions = new FsPermission[srcstat.length];
        for(int i = 0; i < srcstat.length; i++) {
          permissions[i] = new FsPermission((short)(i & 0666));
          fs.setPermission(srcstat[i].getPath(), permissions[i]);
        }

        ToolRunner.run(new DistCp(conf),
            new String[]{"-pp", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(nnUri, "/destdat", files));
  
        FileStatus[] dststat = getFileStatus(nnUri, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, permissions[i], dststat[i].getPermission());
        }
        deldir(nnUri, "/destdat");
        deldir(nnUri, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testMapCount() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = dfs.getFileSystem();
      namenode = fs.getUri().toString();
      mr = new MiniMRCluster(3, namenode, 1);
      MyFile[] files = createFiles(fs.getUri(), "/srcdat");
      long totsize = 0;
      for (MyFile f : files) {
        totsize += f.getSize();
      }
      JobConf job = mr.createJobConf();
      job.setLong("distcp.bytes.per.map", totsize / 3);
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "100",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(namenode, "/destdat", files));
      FileStatus[] logs = fs.listStatus(new Path(namenode+"/logs"));
      // rare case where splits are exact, logs.length can be 4
      assertTrue("Unexpected map count", logs.length == 5 || logs.length == 4);

      deldir(namenode, "/destdat");
      deldir(namenode, "/logs");
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "1",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});
      logs = fs.listStatus(new Path(namenode+"/logs"));
      assertTrue("Unexpected map count", logs.length == 2);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

}
