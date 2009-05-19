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
package org.apache.hadoop.mapred;

import java.io.DataOutputStream;

import junit.framework.TestCase;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestFileInputFormat extends TestCase {

  public void testLocality() throws Exception {
    JobConf conf = new JobConf();
    MiniDFSCluster dfs = null;
    try {
      dfs = new MiniDFSCluster(conf, 4, true,
                               new String[]{"/rack0", "/rack0", 
                                             "/rack1", "/rack1"},
                               new String[]{"host0", "host1", 
                                            "host2", "host3"});
      FileSystem fs = dfs.getFileSystem();
      System.out.println("FileSystem " + fs.getUri());
      Path path = new Path("/foo/bar");
      // create a multi-block file on hdfs
      DataOutputStream out = fs.create(path, true, 4096, 
                                       (short) 2, 512, null);
      for(int i=0; i < 1000; ++i) {
        out.writeChars("Hello\n");
      }
      out.close();
      System.out.println("Wrote file");

      // split it using a file input format
      TextInputFormat.addInputPath(conf, path);
      TextInputFormat inFormat = new TextInputFormat();
      inFormat.configure(conf);
      InputSplit[] splits = inFormat.getSplits(conf, 1);
      FileStatus fileStatus = fs.getFileStatus(path);
      BlockLocation[] locations = 
        fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      System.out.println("Made splits");

      // make sure that each split is a block and the locations match
      for(int i=0; i < splits.length; ++i) {
        FileSplit fileSplit = (FileSplit) splits[i];
        System.out.println("File split: " + fileSplit);
        for (String h: fileSplit.getLocations()) {
          System.out.println("Location: " + h);
        }
        System.out.println("Block: " + locations[i]);
        assertEquals(locations[i].getOffset(), fileSplit.getStart());
        assertEquals(locations[i].getLength(), fileSplit.getLength());
        String[] blockLocs = locations[i].getHosts();
        String[] splitLocs = fileSplit.getLocations();
        assertEquals(2, blockLocs.length);
        assertEquals(2, splitLocs.length);
        assertTrue((blockLocs[0].equals(splitLocs[0]) && 
                    blockLocs[1].equals(splitLocs[1])) ||
                   (blockLocs[1].equals(splitLocs[0]) &&
                    blockLocs[0].equals(splitLocs[1])));
      }
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }

}
