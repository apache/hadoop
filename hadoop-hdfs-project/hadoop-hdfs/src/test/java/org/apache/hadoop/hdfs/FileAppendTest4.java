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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** This is a comprehensive append test that tries
 * all combinations of file length and number of appended bytes
 * In each iteration, it creates a file of len1. Then reopen
 * the file for append. It first append len2 bytes, calls hflush,
 * append len3 bytes and close the file. Afterwards, the content of
 * the file is validated.
 * Len1 ranges from [0, 2*BLOCK_SIZE+1], len2 ranges from [0, BLOCK_SIZE+1],
 * and len3 ranges from [0, BLOCK_SIZE+1].
 *
 */
public class FileAppendTest4 {
  public static final Logger LOG =
      LoggerFactory.getLogger(FileAppendTest4.class);
  
  private static final int BYTES_PER_CHECKSUM = 4;
  private static final int PACKET_SIZE = BYTES_PER_CHECKSUM;
  private static final int BLOCK_SIZE = 2*PACKET_SIZE;
  private static final short REPLICATION = 3;
  private static final int DATANODE_NUM = 5;
  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;

  private static void init(Configuration conf) {
    conf.setInt(HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);
  }
  
  @BeforeClass
  public static void startUp () throws IOException {
    conf = new HdfsConfiguration();
    init(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).build();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Comprehensive test for append 
   * @throws IOException an exception might be thrown
   */
  @Test
  public void testAppend() throws IOException {
    final int maxOldFileLen = 2*BLOCK_SIZE+1;
    final int maxFlushedBytes = BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(
        maxOldFileLen+2*maxFlushedBytes);
    for (int oldFileLen =0; oldFileLen <=maxOldFileLen; oldFileLen++) {
      for (int flushedBytes1=0; flushedBytes1<=maxFlushedBytes; 
                                flushedBytes1++) {
        for (int flushedBytes2=0; flushedBytes2 <=maxFlushedBytes; 
                                  flushedBytes2++) {
          final int fileLen = oldFileLen + flushedBytes1 + flushedBytes2;
          // create the initial file of oldFileLen
          final Path p = 
            new Path("foo"+ oldFileLen +"_"+ flushedBytes1 +"_"+ flushedBytes2);
          LOG.info("Creating file " + p);
          FSDataOutputStream out = fs.create(p, false, 
              conf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096), 
              REPLICATION, BLOCK_SIZE);
          out.write(contents, 0, oldFileLen);
          out.close();

          // append flushedBytes bytes to the file
          out = fs.append(p);
          out.write(contents, oldFileLen, flushedBytes1);
          out.hflush();

          // write another flushedBytes2 bytes to the file
          out.write(contents, oldFileLen + flushedBytes1, flushedBytes2);
          out.close();

          // validate the file content
          AppendTestUtil.checkFullFile(fs, p, fileLen, contents, p.toString());
          fs.delete(p, false);
        }
      }
    }
  }
}
