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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;

/**
 * S3A contract tests covering file seek.
 */
@RunWith(Parameterized.class)
public class ITestS3AContractSeek extends AbstractContractSeekTest {

  protected static final int READAHEAD = 1024;

  private final String seekPolicy;

  /**
   * Test array for parameterized test runs.
   * @return a list of parameter tuples.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {Constants.INPUT_FADV_RANDOM},
        {Constants.INPUT_FADV_NORMAL},
        {Constants.INPUT_FADV_SEQUENTIAL},
    });
  }

  public ITestS3AContractSeek(final String seekPolicy) {
    this.seekPolicy = seekPolicy;
  }
  
  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    conf.setInt(Constants.READAHEAD_RANGE, READAHEAD);
    conf.set(Constants.INPUT_FADVISE, seekPolicy);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Test for HADOOP-16109: Parquet reading S3AFileSystem causes EOF.
   */
  @Test
  public void testReadPastReadahead() throws Throwable {
    Path path = path("testReadPastReadahead");
    byte[] dataset = ContractTestUtils.dataset(READAHEAD * 2, 'a', 32);
    FileSystem fs = getFileSystem();
    ContractTestUtils.writeDataset(fs, path, dataset, dataset.length, READAHEAD,
        true);
    // forward seek reading across readahead boundary
    try (FSDataInputStream in = fs.open(path)) {
      final byte[] temp = new byte[5];
      in.readByte();
      in.readFully(1023, temp); // <-- works
    }
    // forward seek reading from end of readahead boundary
    try (FSDataInputStream in = fs.open(path)) {
      final byte[] temp = new byte[5];
      in.readByte();
      in.readFully(1024, temp); // <-- throws EOFException
    }
  }
}
