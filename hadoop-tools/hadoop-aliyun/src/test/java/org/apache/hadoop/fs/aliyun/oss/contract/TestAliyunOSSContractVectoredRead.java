/*
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

package org.apache.hadoop.fs.aliyun.oss.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.test.MoreAsserts.assertEqual;

public class TestAliyunOSSContractVectoredRead extends AbstractContractVectoredReadTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAliyunOSSContractVectoredRead.class);

  public TestAliyunOSSContractVectoredRead(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new AliyunOSSContract(conf);
  }

  @Test
  public void testMinSeekAndMaxSizeConfigsPropagation() throws Exception {
    Configuration conf = getFileSystem().getConf();
    AliyunOSSTestUtils.removeBaseAndBucketOverrides(conf,
        Constants.OSS_VECTOR_READS_MAX_MERGED_READ_SIZE,
        Constants.OSS_VECTOR_READS_MIN_SEEK_SIZE);
    AliyunOSSTestUtils.disableFilesystemCaching(conf);
    final int configuredMinSeek = 2 * 1024;
    final int configuredMaxSize = 10 * 1024 * 1024;
    conf.setLong(Constants.OSS_VECTOR_READS_MIN_SEEK_SIZE, configuredMinSeek);
    conf.setLong(Constants.OSS_VECTOR_READS_MAX_MERGED_READ_SIZE, configuredMaxSize);
    try (AliyunOSSFileSystem fs = AliyunOSSTestUtils.createTestFileSystem(conf)) {
      try (FSDataInputStream fis = fs.open(path(VECTORED_READ_FILE_NAME))) {
        int newMinSeek = fis.minSeekForVectorReads();
        int newMaxSize = fis.maxReadSizeForVectorReads();
        assertEqual(newMinSeek, configuredMinSeek,
            "configured oss min seek for vectored reads");
        assertEqual(newMaxSize, configuredMaxSize,
            "configured oss max size for vectored reads");
      }
    }
  }

  @Test
  public void testMinSeekAndMaxSizeDefaultValues() throws Exception {
    Configuration conf = getFileSystem().getConf();
    AliyunOSSTestUtils.removeBaseAndBucketOverrides(conf,
        Constants.OSS_VECTOR_READS_MIN_SEEK_SIZE,
        Constants.OSS_VECTOR_READS_MAX_MERGED_READ_SIZE);
    try (AliyunOSSFileSystem fs = AliyunOSSTestUtils.createTestFileSystem(conf)) {
      try (FSDataInputStream fis = fs.open(path(VECTORED_READ_FILE_NAME))) {
        int minSeek = fis.minSeekForVectorReads();
        int maxSize = fis.maxReadSizeForVectorReads();
        assertEqual(minSeek, Constants.DEFAULT_OSS_VECTOR_READS_MIN_SEEK_SIZE,
            "default oss min seek for vectored reads");
        assertEqual(maxSize, Constants.DEFAULT_OSS_VECTOR_READS_MAX_MERGED_READ_SIZE,
            "default oss max read size for vectored reads");
      }
    }
  }
}
