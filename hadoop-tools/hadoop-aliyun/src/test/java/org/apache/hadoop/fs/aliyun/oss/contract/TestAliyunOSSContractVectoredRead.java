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

import com.aliyun.oss.OSSException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.range;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.apache.hadoop.test.MoreAsserts.assertEqual;

public class TestAliyunOSSContractVectoredRead extends AbstractContractVectoredReadTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAliyunOSSContractVectoredRead.class);

  public TestAliyunOSSContractVectoredRead(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    conf.setBoolean(Constants.OSS_USE_HTTP_STANDARD_RANGE, true);
    return new AliyunOSSContract(conf);
  }

  /**
   * Verify response to a vector read request which is beyond the
   * real length of the file.
   * Unlike the {@link #testEOFRanges()} test, the input stream in
   * this test thinks the file is longer than it is, so the call
   * fails in the GET request.
   */
  @Test
  public void testEOFRanges416Handling() throws Exception {
    FileSystem fs = getFileSystem();

    final int extendedLen = DATASET_LEN + 1024;
    CompletableFuture<FSDataInputStream> builder =
        fs.openFile(path(VECTORED_READ_FILE_NAME))
            .mustLong(FS_OPTION_OPENFILE_LENGTH, extendedLen)
            .opt(FS_OPTION_OPENFILE_READ_POLICY,
                FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE)
            .build();
    List<FileRange> fileRanges = range(DATASET_LEN, 100);

    // read starting past EOF generates a 416 response, mapped to
    // RangeNotSatisfiableEOFException
    describe("Read starting from past EOF");
    try (FSDataInputStream in = builder.get()) {
      in.readVectored(fileRanges, getAllocate());
      FileRange res = fileRanges.get(0);
      CompletableFuture<ByteBuffer> data = res.getData();
      interceptFuture(OSSException.class,
          "",
          ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
          TimeUnit.SECONDS,
          data);
    }

    // a read starting before the EOF and continuing past it does generate
    // an EOF exception, but not a 416.
    describe("Read starting 0 continuing past EOF");
    try (FSDataInputStream in = fs.openFile(path(VECTORED_READ_FILE_NAME))
        .mustLong(FS_OPTION_OPENFILE_LENGTH, extendedLen)
        .build().get()) {
      final FileRange range = FileRange.createFileRange(0, extendedLen);
      in.readVectored(Arrays.asList(range), getAllocate());
      CompletableFuture<ByteBuffer> data = range.getData();
      interceptFuture(EOFException.class, "",
          ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
          TimeUnit.SECONDS,
          data);
    }

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
