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

package org.apache.hadoop.fs.azurebfs;

import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;

/**
 * Test For Verifying Checksum Related Operations
 */
public class ITestAzureBlobFileSystemChecksum extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemChecksum() throws Exception {
    super();
  }

  @Test
  public void testWriteReadWithChecksum() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AbfsConfiguration conf = fs.getAbfsStore().getAbfsConfiguration();
    // Enable checksum validations for Read and Write Requests
    conf.setIsChecksumEnabled(true);

    Path testpath = new Path("a/b.txt");
    String dataUploaded = "This is Sample Data";
    FSDataOutputStream out = fs.create(testpath);
    out.write(dataUploaded.getBytes(StandardCharsets.UTF_8));
    out.hflush();
    out.close();

    FSDataInputStream in = fs.open(testpath);
    byte[] bytesRead = new byte[dataUploaded.length()];
    in.read(bytesRead);

    // Verify that the data read is same as data written
    Assertions.assertThat(bytesRead).describedAs("").containsExactly(dataUploaded.getBytes(StandardCharsets.UTF_8));
    Assertions.assertThat(new String(bytesRead, StandardCharsets.UTF_8)).describedAs("").isEqualTo(dataUploaded);
  }
}
