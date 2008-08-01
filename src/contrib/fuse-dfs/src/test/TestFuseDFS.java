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

import org.apache.hadoop.dfs.*;
import org.apache.hadoop.hdfs.*;
import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.net.*;

/**
 * This class tests that the Fuse module for DFS can mount properly
 * and does a few simple commands:
 * mkdir
 * rmdir
 * ls
 * cat
 *
 * cp and touch are purposely not tested because they won't work with the current module

 *
 */
public class TestFuseDFS extends TestCase {

  /**
   * mount the fuse file system using assumed fuse module library installed in /usr/local/lib or somewhere else on your
   * pre-existing LD_LIBRARY_PATH
   *
   */

  static Process fuse_process;
  static String fuse_cmd;
  static private void mount(String mountpoint, URI dfs) throws IOException, InterruptedException  {

    String cp = System.getProperty("java.class.path");
    Runtime r = Runtime.getRuntime();
    fuse_cmd = System.getProperty("build.test") + "/../fuse_dfs";
    String libhdfs = System.getProperty("build.test") + "/../../../libhdfs/";
    String arch = System.getProperty("os.arch");
    String jvm = System.getProperty("java.home") + "/lib/" + arch + "/server";
    String lp = System.getProperty("LD_LIBRARY_PATH") + ":" + "/usr/local/lib:" + libhdfs + ":" + jvm;
    System.err.println("LD_LIBRARY_PATH=" + lp);
    String cmd[] =  {  fuse_cmd, "dfs://" + dfs.getHost() + ":" + String.valueOf(dfs.getPort()), 
		       mountpoint, "-odebug", "-oentry_timeout=1",  "-oattribute_timeout=1", "-ousetrash", "rw" };
    final String [] envp = {
      "CLASSPATH="+  cp,
      "LD_LIBRARY_PATH=" + lp,
      "PATH=" + "/usr/bin:/bin"

    };

    // ensure the mount point is not currently mounted
    Process p = r.exec("fusermount -u " + mountpoint);
    p.waitFor();

    // clean up the mount point
    p = r.exec("rm -rf " + mountpoint);
    assertTrue(p.waitFor() == 0);

    // make the mount point if needed
    p = r.exec("mkdir -p " + mountpoint);
    assertTrue(p.waitFor() == 0);

    // mount fuse to the mount point
    fuse_process = r.exec(cmd, envp);

    // give DFS a chance to come up
    try { Thread.sleep(3000); } catch(Exception e) { }
  }

  /**
   * unmounts fuse for before shutting down.
   */
  static private void umount(String mpoint) throws IOException, InterruptedException {
    Runtime r= Runtime.getRuntime();
    Process p = r.exec("fusermount -u " + mpoint);
    p.waitFor();
  }

  /**
   * Set things up - create mini dfs cluster and mount the fuse filesystem.
   */
  public TestFuseDFS() throws IOException,InterruptedException  {
  }

  static private MiniDFSCluster cluster;
  static private FileSystem fileSys;
  final static private String mpoint;

  static {
    mpoint = System.getProperty("build.test") + "/mnt";
    System.runFinalizersOnExit(true);
    startStuff();
  }


  static public void startStuff() {
    try {
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.permissions",false);
      cluster = new MiniDFSCluster(conf, 1, true, null);
      fileSys = cluster.getFileSystem();
      assertTrue(fileSys.getFileStatus(new Path("/")).isDir());
      mount(mpoint, fileSys.getUri());
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  public void setUp() {
  }

  /**
   * use shell to create a dir and then use filesys to see it exists.
   */
  public void testMkdir() throws IOException,InterruptedException, Exception  {
    try {
      // First create a new directory with mkdirs
      Path path = new Path("/foo");
      Runtime r = Runtime.getRuntime();
      String cmd = "mkdir -p " + mpoint + path.toString();
      Process p = r.exec(cmd);
      assertTrue(p.waitFor() == 0);

      // check it is there
      assertTrue(fileSys.getFileStatus(path).isDir());

      // check again through the shell
      String lsCmd = "ls " + mpoint + path.toString();
      p = r.exec(lsCmd);
      assertTrue(p.waitFor() == 0);
    } catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
  }


  /**
   * use shell to create a dir and then use filesys to see it exists.
   */
  public void testWrites() throws IOException,InterruptedException  {
    try {

      // write a hello file
      File file = new File(mpoint, "hello.txt");
      FileOutputStream f = new FileOutputStream(file);
      String s = "hello ";
      f.write(s.getBytes());
      s = "world";
      f.write(s.getBytes());
      f.flush();
      f.close();

      // check the file exists.
      Path myPath = new Path("/hello.txt");
      assertTrue(fileSys.exists(myPath));

      // check the data is ok
      FileInputStream fi = new FileInputStream(new File(mpoint, "hello.txt"));
      byte b[] = new byte[12];
      int length = fi.read(b,0,12);
      String s2 = new String( b);
    } catch(Exception e) {
      e.printStackTrace();
    } finally {
    }
  }



  /**
   * Test ls for dir already created in testMkdDir also tests bad ls
   */
  public void testLs() throws IOException,InterruptedException  {
    try {
      // First create a new directory with mkdirs
      Runtime r = Runtime.getRuntime();

      // mkdir
      Process p = r.exec("mkdir -p " + mpoint + "/test/mkdirs");
      assertTrue(p.waitFor() == 0);

      // ls
      p = r.exec("ls " + mpoint + "/test/mkdirs");
      assertTrue(p.waitFor() == 0);

      // ls non-existant directory
      p = r.exec("ls " + mpoint + "/test/mkdirsNotThere");
      int res = p.waitFor();
      assertFalse(res == 0);
    } catch(Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Remove a dir using the shell and use filesys to see it no longer exists.
   */
  public void testRmdir() throws IOException,InterruptedException  {
    try {
      // First create a new directory with mkdirs

      Runtime r = Runtime.getRuntime();
      Process p = r.exec("mkdir -p " + mpoint + "/test/mkdirs");
      assertTrue(p.waitFor() == 0);

      Path myPath = new Path("/test/mkdirs");
      assertTrue(fileSys.exists(myPath));

      // remove it
      p = r.exec("rmdir " + mpoint + "/test/mkdirs");
      assertTrue(p.waitFor() == 0);

      // check it is not there
      assertFalse(fileSys.exists(myPath));

      Path trashPath = new Path("/Trash/Current/test/mkdirs");
      assertTrue(fileSys.exists(trashPath));

      // make it again to test trashing same thing twice
      p = r.exec("mkdir -p " + mpoint + "/test/mkdirs");
      assertTrue(p.waitFor() == 0);

      assertTrue(fileSys.exists(myPath));

      // remove it
      p = r.exec("rmdir " + mpoint + "/test/mkdirs");
      assertTrue(p.waitFor() == 0);

      // check it is not there
      assertFalse(fileSys.exists(myPath));

      trashPath = new Path("/Trash/Current/test/mkdirs.1");
      assertTrue(fileSys.exists(trashPath));

    } catch(Exception e) {
      e.printStackTrace();
    }
  }


  /**
   * Use filesys to create the hello world! file and then cat it and see its contents are correct.
   */
  public void testCat() throws IOException,InterruptedException  {
    try {
      // First create a new directory with mkdirs
      Runtime r = Runtime.getRuntime();
      Process p = r.exec("rm -rf " + mpoint + "/test/hello");
      assertTrue(p.waitFor() == 0);

      // create the file
      Path myPath = new Path("/test/hello");
      FSDataOutputStream s = fileSys.create(myPath);
      String hello = "hello world!";
      s.writeUTF(hello);
      s.writeInt(1033);
      s.close();

      // check it exists
      assertTrue(fileSys.exists(myPath));

      // cat the file
      DataInputStream is = new DataInputStream(new FileInputStream(mpoint + "/test/hello"));
      String s2 = DataInputStream.readUTF(is);
      int s3 = is.readInt();
      assertTrue(s2.equals(hello));
      assertTrue(s3 == 1033);

    } catch(Exception e) {
      e.printStackTrace();
    } finally {
      close();
    }
  }


  /**
   * Unmount and close
   */
  protected void tearDown() throws Exception {
  }

  /**
   * Unmount and close
   */
  protected void finalize() throws Throwable {
    close();
  }

  public void close() {
    try {

      // print out the fuse debug output
      {
      InputStream i = fuse_process.getInputStream();
      byte b[] = new byte[i.available()];
      int length = i.read(b);
      System.err.println("read x bytes: " + length);
      System.err.write(b,0,b.length);
      }

      int length;
      do {
      InputStream i = fuse_process.getErrorStream();
      byte b[] = new byte[i.available()];
      length = i.read(b);
      System.err.println("read x bytes: " + length);
      System.err.write(b,0,b.length);
      } while(length > 0) ;

      umount(mpoint);

      fuse_process.destroy();
      fuse_process = null;
      if(fileSys != null) {
        fileSys.close();
        fileSys = null;
      }
      if(cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    } catch(Exception e) { }
  }
};
