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
    private void mount(String mountpoint, URI dfs) throws IOException, InterruptedException  {
        String cp = System.getenv("CLASSPATH");
        String libhdfs = "../../c++/libhdfs/";
        String lp = System.getenv("LD_LIBRARY_PATH") + ":" + "/usr/local/lib:" + libhdfs;

        Runtime r = Runtime.getRuntime();
        String fuse_cmd = "../src/fuse_dfs";

        String cmd = fuse_cmd;
        cmd += " ";
        //        cmd += dfs.toASCIIString();
        cmd += "dfs://";
        cmd += dfs.getHost();
        cmd += ":" ;
        cmd += String.valueOf(dfs.getPort());
        cmd += " ";
        cmd += mountpoint;
        final String [] envp = {
            "CLASSPATH="+  cp,
            "LD_LIBRARY_PATH=" + lp
        };


        // ensure the mount point is not currently mounted
        Process p = r.exec("sudo umount -l " + mountpoint);
        p.waitFor();

        // make the mount point if needed
        p = r.exec("mkdir -p " + mountpoint);
        assertTrue(p.waitFor() == 0);

        System.err.println("cmd=" + cmd);
        // mount fuse to the mount point
        p = r.exec(cmd, envp);
        assertTrue(p.waitFor() == 0);
        assertTrue(p.exitValue() == 0);
    }

    /**
     * unmounts fuse for before shutting down.
     */
    private void umount(String mpoint) throws IOException, InterruptedException {
        Runtime r= Runtime.getRuntime();
        Process p = r.exec("sudo umount -l " + mpoint);
        p.waitFor();
    }

    /**
     * Set things up - create mini dfs cluster and mount the fuse filesystem.
     */
    public TestFuseDFS() throws IOException,InterruptedException  {
        Configuration conf = new Configuration();
        this.cluster = new MiniDFSCluster(conf, 1, true, null);
        this.fileSys = this.cluster.getFileSystem();
        String mpoint = "/tmp/testfuse";
        this.mount(mpoint, fileSys.getUri());
        this.myPath = new Path("/test/mkdirs");
    }

    private MiniDFSCluster cluster;
    private FileSystem fileSys;
    private String mpoint = "/tmp/testfuse";
    private Path myPath;


    /**
     * use shell to create a dir and then use filesys to see it exists.
     */
    public void testMkdir() throws IOException,InterruptedException  {
        // First create a new directory with mkdirs
        Runtime r = Runtime.getRuntime();
        Process p = r.exec("mkdir -p " + mpoint + "/test/mkdirs");
        assertTrue(p.waitFor() == 0);
        assertTrue(p.exitValue() == 0);

        assertTrue(this.fileSys.exists(myPath));


    }

    /**
     * Test ls for dir already created in testMkdDir also tests bad ls
     */
    public void testLs() throws IOException,InterruptedException  {
        // First create a new directory with mkdirs
        Runtime r = Runtime.getRuntime();
        Process p = r.exec("ls " + mpoint + "/test/mkdirs");
        assertTrue(p.waitFor() == 0);
        assertTrue(p.exitValue() == 0);

        p = r.exec("ls " + mpoint + "/test/mkdirsNotThere");
        assertFalse(p.waitFor() == 0);
        assertFalse(p.exitValue() == 0);

    }

    /**
     * Remove a dir using the shell and use filesys to see it no longer exists.
     */
    public void testRmdir() throws IOException,InterruptedException  {
        // First create a new directory with mkdirs
        Path myPath = new Path("/test/mkdirs");
        assertTrue(fileSys.exists(myPath));

        Runtime r = Runtime.getRuntime();
        Process p = r.exec("rmdir " + mpoint + "/test/mkdirs");

        assertTrue(p.waitFor() == 0);
        assertTrue(p.exitValue() == 0);

        assertFalse(fileSys.exists(myPath));
    }


    /**
     * Use filesys to create the hello world! file and then cat it and see its contents are correct.
     */
    public void testCat() throws IOException,InterruptedException  {
        // First create a new directory with mkdirs

        Path myPath = new Path("/test/hello");
        FSDataOutputStream s = fileSys.create(myPath);
        String hello = "hello world!";

        s.write(hello.getBytes());
        s.close();

        assertTrue(fileSys.exists(myPath));

        Runtime r = Runtime.getRuntime();
        Process p = r.exec("cat " + mpoint + "/test/hello");

        assertTrue(p.waitFor() == 0);
        assertTrue(p.exitValue() == 0);
        InputStream i = p.getInputStream();
        byte b[] = new byte[1024];
        int length = i.read(b);
        String s2 = new String(b,0,length);
        assertTrue(s2.equals(hello));
    }


    /**
     * Unmount and close
    */
    public void finalize() {
        try {
            this.close();
        } catch(Exception e) { }
    }

    /**
     * Unmount and close
    */
    public void close() throws IOException, InterruptedException {
        this.umount(mpoint);
        if(this.fileSys != null) {
            this.fileSys.close();
            this.fileSys = null;
        }
        if(this.cluster != null) {
            this.cluster.shutdown();
            this.cluster = null;
        }
    }

    public  static void main(String args[]) {
        try {
            TestFuseDFS d = new TestFuseDFS();
            d.testMkdir();
            d.testLs();
            d.testRmdir();
            d.testCat();
            d.close();
        } catch(Exception e) {
            System.err.println("e=" + e.getMessage());
            e.printStackTrace();
        }

    }
}
