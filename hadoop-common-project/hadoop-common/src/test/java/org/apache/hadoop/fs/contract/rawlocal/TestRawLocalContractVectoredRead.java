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

package org.apache.hadoop.fs.contract.rawlocal;

import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

@ParameterizedClass(name="buffer-{0}")
@MethodSource("params")
public class TestRawLocalContractVectoredRead extends AbstractContractVectoredReadTest {

  public TestRawLocalContractVectoredRead(final String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new RawlocalFSContract(conf);
  }
}
