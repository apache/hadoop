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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.BLOCK_SIZE;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.FILE_LENGTHS;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.NUM_DATA_UNITS;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.NUM_PARITY_UNITS;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.initializeCluster;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.tearDownCluster;

/**
 * Test online recovery with failed DNs. This test is parameterized.
 */
@Timeout(300)
public class TestReadStripedFileWithDNFailure {
  static final Logger LOG =
      LoggerFactory.getLogger(TestReadStripedFileWithDNFailure.class);

  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  public static void setup() throws IOException {
    cluster = initializeCluster();
    dfs = cluster.getFileSystem();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    tearDownCluster(cluster);
  }

  public static Collection<Object[]> getParameters() {
    ArrayList<Object[]> params = new ArrayList<>();
    for (int fileLength : FILE_LENGTHS) {
      for (int i = 0; i < NUM_PARITY_UNITS; i++) {
        params.add(new Object[] {fileLength, i+1});
      }
    }
    return params;
  }

  private int fileLength;
  private int dnFailureNum;

  public void initTestReadStripedFileWithDNFailure(int pFileLength, int pDnFailureNum)
      throws IOException {
    this.fileLength = fileLength;
    this.dnFailureNum = dnFailureNum;
  }

  /**
   * Shutdown tolerable number of Datanode before reading.
   * Verify the decoding works correctly.
   */
  @MethodSource("getParameters")
  @ParameterizedTest
  public void testReadWithDNFailure(int pFileLength, int pDnFailureNum)
      throws Exception {
    try {
      initTestReadStripedFileWithDNFailure(pFileLength, pDnFailureNum);
      // setup a new cluster with no dead datanode
      setup();
      ReadStripedFileWithDecodingHelper.testReadWithDNFailure(cluster,
          dfs, fileLength, dnFailureNum);
    } catch (IOException ioe) {
      String fileType = fileLength < (BLOCK_SIZE * NUM_DATA_UNITS) ?
          "smallFile" : "largeFile";
      LOG.error("Failed to read file with DN failure:"
          + " fileType = " + fileType
          + ", dnFailureNum = " + dnFailureNum);
    } finally {
      // tear down the cluster
      tearDown();
    }
  }
}
