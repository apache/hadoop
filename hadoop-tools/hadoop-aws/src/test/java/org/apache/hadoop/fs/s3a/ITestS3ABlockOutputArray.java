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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;

import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Tests small file upload functionality for
 * {@link S3ABlockOutputStream} with the blocks buffered in byte arrays.
 *
 * File sizes are kept small to reduce test duration on slow connections;
 * multipart tests are kept in scale tests.
 */
public class ITestS3ABlockOutputArray extends AbstractS3ATestBase {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setLong(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    conf.setBoolean(Constants.FAST_UPLOAD, true);
    conf.set(FAST_UPLOAD_BUFFER, getBlockOutputBufferName());
    return conf;
  }

  protected String getBlockOutputBufferName() {
    return FAST_UPLOAD_BUFFER_ARRAY;
  }

  @Test
  public void testZeroByteUpload() throws IOException {
    verifyUpload("0", 0);
  }

  @Test
  public void testRegularUpload() throws IOException {
    verifyUpload("regular", 1024);
  }

  @Test(expected = IOException.class)
  public void testDoubleStreamClose() throws Throwable {
    Path dest = path("testDoubleStreamClose");
    describe(" testDoubleStreamClose");
    FSDataOutputStream stream = getFileSystem().create(dest, true);
    byte[] data = ContractTestUtils.dataset(16, 'a', 26);
    try {
      stream.write(data);
      stream.close();
      stream.write(data);
    } finally {
      IOUtils.closeStream(stream);
    }
  }

  public void verifyUpload(String name, int fileSize) throws IOException {
    Path dest = path(name);
    describe(name + " upload to " + dest);
    ContractTestUtils.createAndVerifyFile(
        getFileSystem(),
        dest,
        fileSize);
  }
}
