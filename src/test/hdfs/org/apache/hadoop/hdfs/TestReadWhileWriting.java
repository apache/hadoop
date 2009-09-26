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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

/** Test reading from hdfs while a file is being written. */
public class TestReadWhileWriting {
  {
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  private static final String DIR = "/"
      + TestReadWhileWriting.class.getSimpleName() + "/";
  private static final int BLOCK_SIZE = 8192;

  /** Test reading while writing. */
  @Test
  public void testReadWhileWriting() throws Exception {
    Configuration conf = new Configuration();
    // create cluster
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    try {
      cluster.waitActive();
      final FileSystem fs = cluster.getFileSystem();

      // write to a file but not closing it.
      final Path p = new Path(DIR, "file1");
      final FSDataOutputStream out = fs.create(p, true,
          fs.getConf().getInt("io.file.buffer.size", 4096),
          (short)3, BLOCK_SIZE);
      final int size = BLOCK_SIZE/3;
      final byte[] buffer = AppendTestUtil.randomBytes(0, size);
      out.write(buffer, 0, size);
      out.flush();
      out.sync();

      // able to read?
      Assert.assertTrue(read(fs, p, size));

      out.close();
    } finally {
      cluster.shutdown();
    }
  }

  /** able to read? */
  private static boolean read(FileSystem fs, Path p, int expectedsize
      ) throws Exception {
    //try at most 3 minutes
    for(int i = 0; i < 360; i++) {
      final FSDataInputStream in = fs.open(p);
      try {
        final int available = in.available();
        System.out.println(i + ") in.available()=" + available);
        Assert.assertTrue(available >= 0);
        Assert.assertTrue(available <= expectedsize);
        if (available == expectedsize) {
          return true;
        }
      } finally {
        in.close();
      }
      Thread.sleep(500);
    }
    return false;
  }
}
