/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.contract.hdfs;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractMultipartUploaderTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * Test MultipartUploader tests on HDFS.
 */
public class TestHDFSContractMultipartUploader extends
    AbstractContractMultipartUploaderTest {

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestHDFSContractMultipartUploader.class);

  @BeforeClass
  public static void createCluster() throws IOException {
    HDFSContract.createCluster();
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    HDFSContract.destroyCluster();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new HDFSContract(conf);
  }

  /**
   * HDFS doesn't have any restriction on the part size.
   * @return 1KB
   */
  @Override
  protected int partSizeInBytes() {
    return 1024;
  }

  @Override
  protected boolean finalizeConsumesUploadIdImmediately() {
    return true;
  }

  @Override
  protected boolean supportsConcurrentUploadsToSamePath() {
    return true;
  }
}
