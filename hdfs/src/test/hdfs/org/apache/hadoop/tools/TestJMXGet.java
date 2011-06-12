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

package org.apache.hadoop.tools;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.JMXGet;


/**
 * Startup and checkpoint tests
 * 
 */
public class TestJMXGet extends TestCase {

  private Configuration config;
  private MiniDFSCluster cluster;

  static final long seed = 0xAAAAEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;

  private void writeFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
        fileSys.getConf().getInt("io.file.buffer.size", 4096),
        (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }


  protected void setUp() throws Exception {
    config = new HdfsConfiguration();
  }

  /**
   * clean up
   */
  public void tearDown() throws Exception {
    if(cluster.isClusterUp())
      cluster.shutdown();

    File data_dir = new File(cluster.getDataDirectory());
    if(data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
      throw new IOException("Could not delete hdfs directory in tearDown '"
          + data_dir + "'");
    }
  }

  /**
   * test JMX connection to NameNode..
   * @throws Exception 
   */
  public void testNameNode() throws Exception {
    int numDatanodes = 2;
    cluster = new MiniDFSCluster(0, config, numDatanodes, true, true, null, 
        null, null);
    cluster.waitActive();

    writeFile(cluster.getFileSystem(), new Path("/test1"), 2);

    JMXGet jmx = new JMXGet();
    jmx.init();


    //get some data from different sources
    int blocks_corrupted = NameNode.getNameNodeMetrics().
    numBlocksCorrupted.get();
    assertEquals(Integer.parseInt(
        jmx.getValue("NumLiveDataNodes")), 2);
    assertEquals(Integer.parseInt(
        jmx.getValue("BlocksCorrupted")), blocks_corrupted);
    assertEquals(Integer.parseInt(
        jmx.getValue("NumOpenConnections")), 0);

    cluster.shutdown();
  }

  /**
   * test JMX connection to DataNode..
   * @throws Exception 
   */
  public void testDataNode() throws Exception {
    int numDatanodes = 2;
    cluster = new MiniDFSCluster(0, config, numDatanodes, true, true, null,
        null, null);
    cluster.waitActive();

    writeFile(cluster.getFileSystem(), new Path("/test"), 2);

    JMXGet jmx = new JMXGet();
    jmx.setService("DataNode");
    jmx.init();
    assertEquals(Integer.parseInt(jmx.getValue("bytes_written")), 0);

    cluster.shutdown();
  }
}
