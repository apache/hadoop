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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestDFSRename extends junit.framework.TestCase {
  static int countLease(MiniDFSCluster cluster) {
    return cluster.getNameNode().namesystem.leaseManager.countLease();
  }
  
  final Path dir = new Path("/test/rename/");

  void list(FileSystem fs, String name) throws IOException {
    FileSystem.LOG.info("\n\n" + name);
    for(FileStatus s : fs.listStatus(dir)) {
      FileSystem.LOG.info("" + s.getPath());
    }
  }

  public void testRename() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    try {
      FileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdirs(dir));
      
      { //test lease
        Path a = new Path(dir, "a");
        Path aa = new Path(dir, "aa");
        Path b = new Path(dir, "b");
  
        DataOutputStream a_out = fs.create(a);
        a_out.writeBytes("something");
        a_out.close();
        
        //should not have any lease
        assertEquals(0, countLease(cluster)); 
  
        DataOutputStream aa_out = fs.create(aa);
        aa_out.writeBytes("something");
  
        //should have 1 lease
        assertEquals(1, countLease(cluster)); 
        list(fs, "rename0");
        fs.rename(a, b);
        list(fs, "rename1");
        aa_out.writeBytes(" more");
        aa_out.close();
        list(fs, "rename2");
  
        //should not have any lease
        assertEquals(0, countLease(cluster));
      }

      { // test non-existent destination
        Path dstPath = new Path("/c/d");
        assertFalse(fs.exists(dstPath));
        assertFalse(fs.rename(dir, dstPath));
      }

      { // test rename /a/b to /a/b/c
        Path src = new Path("/a/b");
        Path dst = new Path("/a/b/c");

        DataOutputStream a_out = fs.create(new Path(src, "foo"));
        a_out.writeBytes("something");
        a_out.close();
        
        assertFalse(fs.rename(src, dst));
      }
      
      fs.delete(dir, true);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
