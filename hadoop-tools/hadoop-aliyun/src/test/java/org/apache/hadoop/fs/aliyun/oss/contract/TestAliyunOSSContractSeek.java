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

package org.apache.hadoop.fs.aliyun.oss.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

/**
 * Aliyun OSS contract seeking tests.
 */
public class TestAliyunOSSContractSeek extends AbstractContractSeekTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new AliyunOSSContract(conf);
  }

  @Test
  public void testSeekBeyondDownloadSize() throws Throwable {
    describe("seek and read beyond download size.");

    Path byteFile = path("byte_file.txt");
    // 'fs.oss.multipart.download.size' = 100 * 1024
    byte[] block = dataset(100 * 1024 + 10, 0, 255);
    FileSystem fs = getFileSystem();
    createFile(fs, byteFile, true, block);

    FSDataInputStream instream = getFileSystem().open(byteFile);
    instream.seek(100 * 1024 - 1);
    assertEquals(100 * 1024 - 1, instream.getPos());
    assertEquals(144, instream.read());
    instream.seek(100 * 1024 + 1);
    assertEquals(100 * 1024 + 1, instream.getPos());
    assertEquals(146, instream.read());
  }
}
