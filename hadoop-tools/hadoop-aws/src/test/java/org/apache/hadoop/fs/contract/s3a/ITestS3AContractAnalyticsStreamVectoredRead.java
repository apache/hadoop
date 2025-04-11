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

package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableAnalyticsAccelerator;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipForAnyEncryptionExceptSSES3;

/**
 * S3A contract tests for vectored reads with the Analytics stream.
 * The analytics stream does not explicitly implement the vectoredRead() method,
 * or currently do and vectored-read specific optimisations
 * (such as range coalescing). However, this test ensures that the base
 * implementation of readVectored {@link org.apache.hadoop.fs.PositionedReadable}
 * still works.
 */
public class ITestS3AContractAnalyticsStreamVectoredRead extends AbstractContractVectoredReadTest {

  public ITestS3AContractAnalyticsStreamVectoredRead(String bufferType) {
    super(bufferType);
  }

  /**
   * Create a configuration.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    enableAnalyticsAccelerator(conf);
    // If encryption is set, some AAL tests will fail.
    // This is because AAL caches the head request response, and uses
    // the eTag when making a GET request. When using encryption, the eTag is
    // no longer a hash of the object content, and is not always the same when
    // the same object is created multiple times. This test creates the file
    // vectored_file.txt before running each test, which will have a
    // different eTag when using encryption, leading to preconditioned failures.
    // This issue is tracked in:
    // https://github.com/awslabs/analytics-accelerator-s3/issues/218
    skipForAnyEncryptionExceptSSES3(conf);
    conf.set("fs.contract.vector-io-early-eof-check", "false");
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }
}
