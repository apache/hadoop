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
package org.apache.hadoop.fs.azurebfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Contract test for vectored reads through ABFS connector.
 */
@ParameterizedClass(name="buffer-{0}")
@MethodSource("params")
public class ITestAbfsFileSystemContractVectoredRead
    extends AbstractContractVectoredReadTest {

  private final boolean isSecure;
  private final ABFSContractTestBinding binding;

  public ITestAbfsFileSystemContractVectoredRead(final String bufferType) throws Exception {
    super(bufferType);
    this.binding = new ABFSContractTestBinding();
    this.isSecure = binding.isSecureMode();
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return this.binding.getRawConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new AbfsFileSystemContract(conf, this.isSecure);
  }
}
