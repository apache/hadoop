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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.initializeCluster;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.tearDownCluster;

/**
 * Test online recovery with files with deleted blocks. This test is
 * parameterized.
 */
@Timeout(300)
public class TestReadStripedFileWithDecodingDeletedData {
  static final Logger LOG =
      LoggerFactory.getLogger(TestReadStripedFileWithDecodingDeletedData.class);

  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  @BeforeAll
  public static void setup() throws IOException {
    cluster = initializeCluster();
    dfs = cluster.getFileSystem();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    tearDownCluster(cluster);
  }

  public static Collection<Object[]> getParameters() {
    return ReadStripedFileWithDecodingHelper.getParameters();
  }

  private int fileLength;
  private int dataDelNum;
  private int parityDelNum;

  public void initTestReadStripedFileWithDecodingDeletedData(int pFileLength, int
      pDataDelNum, int pParityDelNum) {
    this.fileLength = pFileLength;
    this.dataDelNum = pDataDelNum;
    this.parityDelNum = pParityDelNum;
  }

  /**
   * Delete tolerable number of block before reading.
   * Verify the decoding works correctly.
   */
  @ParameterizedTest
  @MethodSource("getParameters")
  public void testReadCorruptedDataByDeleting(int pFileLength, int
      pDataDelNum, int pParityDelNum) throws IOException {
    initTestReadStripedFileWithDecodingDeletedData(pFileLength, pDataDelNum, pParityDelNum);
    String src = "/deleted_" + dataDelNum + "_" + parityDelNum;
    ReadStripedFileWithDecodingHelper.testReadWithBlockCorrupted(cluster,
        dfs, src, fileLength, dataDelNum, parityDelNum, true);
  }
}
