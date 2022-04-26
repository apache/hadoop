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

package org.apache.hadoop.fs.contract.s3a;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileRangeImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.test.MoreAsserts.assertEqual;

public class ITestS3AContractVectoredRead extends AbstractContractVectoredReadTest {

  public ITestS3AContractVectoredRead(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Overriding in S3 vectored read api fails fast in case of EOF
   * requested range.
   */
  @Override
  public void testEOFRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(DATASET_LEN, 100));
    testExceptionalVectoredRead(fs, fileRanges, "EOFException is expected");
  }

  @Test
  public void testMinSeekAndMaxSizeConfigsPropagation() throws Exception {
    Configuration conf = getFileSystem().getConf();
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
            Constants.AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE,
            Constants.AWS_S3_VECTOR_READS_MIN_SEEK_SIZE);
    S3ATestUtils.disableFilesystemCaching(conf);
    final int configuredMinSeek = 2 * 1024;
    final int configuredMaxSize = 10 * 1024 * 1024;
    conf.set(Constants.AWS_S3_VECTOR_READS_MIN_SEEK_SIZE, "2K");
    conf.set(Constants.AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE, "10M");
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      try (FSDataInputStream fis = fs.open(path(VECTORED_READ_FILE_NAME))) {
        int newMinSeek = fis.minSeekForVectorReads();
        int newMaxSize = fis.maxReadSizeForVectorReads();
        assertEqual(newMinSeek, configuredMinSeek,
                "configured s3a min seek for vectored reads");
        assertEqual(newMaxSize, configuredMaxSize,
                "configured s3a max size for vectored reads");
      }
    }
  }

  @Test
  public void testMinSeekAndMaxSizeDefaultValues() throws Exception {
    Configuration conf = getFileSystem().getConf();
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
            Constants.AWS_S3_VECTOR_READS_MIN_SEEK_SIZE,
            Constants.AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE);
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      try (FSDataInputStream fis = fs.open(path(VECTORED_READ_FILE_NAME))) {
        int minSeek = fis.minSeekForVectorReads();
        int maxSize = fis.maxReadSizeForVectorReads();
        assertEqual(minSeek, Constants.DEFAULT_AWS_S3_VECTOR_READS_MIN_SEEK_SIZE,
                "default s3a min seek for vectored reads");
        assertEqual(maxSize, Constants.DEFAULT_AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE,
                "default s3a max read size for vectored reads");
      }
    }
  }
}
