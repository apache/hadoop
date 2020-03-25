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

package org.apache.hadoop.fs.azurebfs.contract;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.contract.AbstractContractUnbufferTest;

/**
 * Contract test for unbuffer operation.
 */
public class ITestAbfsContractUnbuffer extends AbstractContractUnbufferTest {
  private final boolean isSecure;
  private final ABFSContractTestBinding binding;

  public ITestAbfsContractUnbuffer() throws Exception {
    binding = new ABFSContractTestBinding();
    this.isSecure = binding.isSecureMode();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return binding.getRawConfiguration();
  }

  @Override
  protected AbfsFileSystemContract createContract(Configuration conf) {
    return new AbfsFileSystemContract(conf, isSecure);
  }

  /**
   * {@link org.apache.hadoop.fs.azurebfs.services.AbfsInputStream} does not
   * allow calling {@link org.apache.hadoop.fs.Seekable#getPos()} on a closed
   * stream, so this test needs to be overridden so that it does not call
   * getPos() after the stream has been closed.
   */
  @Override
  public void testUnbufferOnClosedFile() throws IOException {
    describe("unbuffer a file before a read");
    FSDataInputStream stream = null;
    try {
      stream = getFileSystem().open(getFile());
      validateFullFileContents(stream);
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
    if (stream != null) {
      stream.unbuffer();
    }
  }
}
