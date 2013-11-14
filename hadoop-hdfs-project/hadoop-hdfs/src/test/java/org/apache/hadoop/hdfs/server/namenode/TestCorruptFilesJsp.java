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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.junit.Test;

/** A JUnit test for corrupt_files.jsp */
public class TestCorruptFilesJsp  {

  @Test
  public void testCorruptFilesJsp() throws Exception {
    MiniDFSCluster cluster = null;
    try {

      final int FILE_SIZE = 512;

      Path[] filepaths = { new Path("/audiobook"), new Path("/audio/audio1"),
          new Path("/audio/audio2"), new Path("/audio/audio") };

      Configuration conf = new HdfsConfiguration();
      // datanode scans directories
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
      // datanode sends block reports
      conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3 * 1000);
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      FileSystem fs = cluster.getFileSystem();

      // create files
      for (Path filepath : filepaths) {
        DFSTestUtil.createFile(fs, filepath, FILE_SIZE, (short) 1, 0L);
        DFSTestUtil.waitReplication(fs, filepath, (short) 1);
      }

      // verify there are not corrupt files
      final NameNode namenode = cluster.getNameNode();
      Collection<FSNamesystem.CorruptFileBlockInfo> badFiles = namenode.
        getNamesystem().listCorruptFileBlocks("/", null);
      assertTrue("There are " + badFiles.size()
          + " corrupt files, but expecting none", badFiles.size() == 0);

      // Check if webui agrees
      URL url = new URL("http://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY)
          + "/corrupt_files.jsp");
      String corruptFilesPage = DFSTestUtil.urlGet(url);
      assertTrue("Corrupt files page is not showing a healthy filesystem",
          corruptFilesPage.contains("No missing blocks found at the moment."));

      // Now corrupt all the files except for the last one
      for (int idx = 0; idx < filepaths.length - 1; idx++) {
        ExtendedBlock blk = DFSTestUtil.getFirstBlock(fs, filepaths[idx]);
        assertTrue(TestDatanodeBlockScanner.corruptReplica(blk, 0));

        // read the file so that the corrupt block is reported to NN
        FSDataInputStream in = fs.open(filepaths[idx]);
        try {
          in.readFully(new byte[FILE_SIZE]);
        } catch (ChecksumException ignored) { // checksum error is expected.
        }
        in.close();
      }

      try {
        Thread.sleep(3000); // Wait for block reports. They shouldn't matter.
      } catch (InterruptedException ie) {}

      // verify if all corrupt files were reported to NN
      badFiles = namenode.getNamesystem().listCorruptFileBlocks("/", null);
      assertTrue("Expecting 3 corrupt files, but got " + badFiles.size(),
          badFiles.size() == 3);

      // Check if webui agrees
      url = new URL("http://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY)
          + "/corrupt_files.jsp");
      corruptFilesPage = DFSTestUtil.urlGet(url);
      assertTrue("'/audiobook' should be corrupt", corruptFilesPage
          .contains("/audiobook"));
      assertTrue("'/audio/audio1' should be corrupt", corruptFilesPage
          .contains("/audio/audio1"));
      assertTrue("'/audio/audio2' should be corrupt", corruptFilesPage
          .contains("/audio/audio2"));
      assertTrue("Summary message shall report 3 corrupt files",
          corruptFilesPage.contains("At least 3 corrupt file(s)"));

      // clean up
      for (Path filepath : filepaths) {
        fs.delete(filepath, false);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

  }

}
