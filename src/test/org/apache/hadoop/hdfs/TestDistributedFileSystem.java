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
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);
    URI address = FileSystem.getDefaultUri(conf);

    try {
      FileSystem.closeAll();

      conf = new Configuration();
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
    Configuration conf = new Configuration();
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
    Configuration conf = new Configuration();
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

    final Configuration conf = new Configuration();
    conf.set("slave.host.name", "localhost");

    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem hdfs = cluster.getFileSystem();
    final String hftpuri = "hftp://" + conf.get("dfs.http.address");
    System.out.println("hftpuri=" + hftpuri);
    final FileSystem hftp = new Path(hftpuri).getFileSystem(conf);

    final String dir = "/filechecksum";
    final int block_size = 1024;
    final int buffer_size = conf.getInt("io.file.buffer.size", 4096);
    conf.setInt("io.bytes.per.checksum", 512);

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
