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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;

public class TestDistributedFileSystem extends junit.framework.TestCase {
  private static final Random RAN = new Random();

  public void testFileSystemCloseAll() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);
    URI address = FileSystem.getDefaultUri(conf);

    try {
      FileSystem.closeAll();

      conf = new HdfsConfiguration();
      FileSystem.setDefaultUri(conf, address);
      FileSystem.get(conf);
      FileSystem.get(conf);
      FileSystem.closeAll();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Tests DFSClient.close throws no ConcurrentModificationException if 
   * multiple files are open.
   */
  public void testDFSClose() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fileSys = cluster.getFileSystem();

    try {
      // create two files
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.create(new Path("/test/dfsclose/file-1"));

      fileSys.close();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  public void testDFSClient() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final Path filepath = new Path("/test/LeaseChecker/foo");
      final long millis = System.currentTimeMillis();

      {
        DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
        assertFalse(dfs.dfs.isLeaseCheckerStarted());
  
        //create a file
        FSDataOutputStream out = dfs.create(filepath);
        assertTrue(dfs.dfs.isLeaseCheckerStarted());
  
        //write something and close
        out.writeLong(millis);
        assertTrue(dfs.dfs.isLeaseCheckerStarted());
        out.close();
        assertTrue(dfs.dfs.isLeaseCheckerStarted());
        dfs.close();
      }

      {
        // Check to see if opening a non-existent file triggers a FNF
        FileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/wrwelkj");
        assertFalse("File should not exist for test.", fs.exists(dir));

        try {
          FSDataInputStream in = fs.open(dir);
          try {
            in.close();
            fs.close();
          } finally {
            assertTrue("Did not get a FileNotFoundException for non-existing" +
                " file.", false);
          }
        } catch (FileNotFoundException fnf) {
          // This is the proper exception to catch; move on.
        }

      }

      {
        DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
        assertFalse(dfs.dfs.isLeaseCheckerStarted());

        //open and check the file
        FSDataInputStream in = dfs.open(filepath);
        assertFalse(dfs.dfs.isLeaseCheckerStarted());
        assertEquals(millis, in.readLong());
        assertFalse(dfs.dfs.isLeaseCheckerStarted());
        in.close();
        assertFalse(dfs.dfs.isLeaseCheckerStarted());
        dfs.close();
      }
      
      { // test accessing DFS with ip address. should work with any hostname
        // alias or ip address that points to the interface that NameNode
        // is listening on. In this case, it is localhost.
        String uri = "hdfs://127.0.0.1:" + cluster.getNameNodePort() + 
                      "/test/ipAddress/file";
        Path path = new Path(uri);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataOutputStream out = fs.create(path);
        byte[] buf = new byte[1024];
        out.write(buf);
        out.close();
        
        FSDataInputStream in = fs.open(path);
        in.readFully(buf);
        in.close();
        fs.close();
      }
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  public void testFileChecksum() throws IOException {
    ((Log4JLogger)HftpFileSystem.LOG).getLogger().setLevel(Level.ALL);

    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "localhost");

    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem hdfs = cluster.getFileSystem();
    final String hftpuri = "hftp://" + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    System.out.println("hftpuri=" + hftpuri);
    final FileSystem hftp = new Path(hftpuri).getFileSystem(conf);

    final String dir = "/filechecksum";
    final int block_size = 1024;
    final int buffer_size = conf.getInt("io.file.buffer.size", 4096);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);

    //try different number of blocks
    for(int n = 0; n < 5; n++) {
      //generate random data
      final byte[] data = new byte[RAN.nextInt(block_size/2-1)+n*block_size+1];
      RAN.nextBytes(data);
      System.out.println("data.length=" + data.length);
  
      //write data to a file
      final Path foo = new Path(dir, "foo" + n);
      {
        final FSDataOutputStream out = hdfs.create(foo, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }
      
      //compute checksum
      final FileChecksum hdfsfoocs = hdfs.getFileChecksum(foo);
      System.out.println("hdfsfoocs=" + hdfsfoocs);
      
      final FileChecksum hftpfoocs = hftp.getFileChecksum(foo);
      System.out.println("hftpfoocs=" + hftpfoocs);

      final Path qualified = new Path(hftpuri + dir, "foo" + n);
      final FileChecksum qfoocs = hftp.getFileChecksum(qualified);
      System.out.println("qfoocs=" + qfoocs);

      //write another file
      final Path bar = new Path(dir, "bar" + n);
      {
        final FSDataOutputStream out = hdfs.create(bar, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }
  
      { //verify checksum
        final FileChecksum barcs = hdfs.getFileChecksum(bar);
        final int barhashcode = barcs.hashCode();
        assertEquals(hdfsfoocs.hashCode(), barhashcode);
        assertEquals(hdfsfoocs, barcs);

        assertEquals(hftpfoocs.hashCode(), barhashcode);
        assertEquals(hftpfoocs, barcs);

        assertEquals(qfoocs.hashCode(), barhashcode);
        assertEquals(qfoocs, barcs);
      }
    }
  }
}
