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

package org.apache.hadoop.fs.tosfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.RawFileSystem;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.object.exceptions.ChecksumMismatchException;
import org.apache.hadoop.fs.tosfs.util.Range;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

public class TestOpen extends AbstractContractOpenTest {

  @BeforeAll
  public static void before() {
    Assumptions.assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new TosContract(conf);
  }

  @Test
  public void testOpenAExpiredFile() throws IOException {
    Path file = path("testOpenAOutageFile");
    FileSystem fs = getFileSystem();
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(fs, file, data, data.length, 1024 * 1024, true);

    FileStatus fileStatus = fs.getFileStatus(file);
    if (fs instanceof RawFileSystem) {
      byte[] expectChecksum = ((RawFileStatus) fileStatus).checksum();
      FSDataInputStream fsDataInputStream =
          ((RawFileSystem) fs).open(file, expectChecksum, Range.of(0, Long.MAX_VALUE));
      fsDataInputStream.close();

      // update the file
      data = dataset(512, 'a', 'z');
      writeDataset(fs, file, data, data.length, 1024 * 1024, true);

      FSDataInputStream newStream =
          ((RawFileSystem) fs).open(file, expectChecksum, Range.of(0, Long.MAX_VALUE));
      assertThrows(ChecksumMismatchException.class, () -> newStream.read(), "the file is expired");
    }
  }
}
