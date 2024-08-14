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

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractUnbufferTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.PREFETCH_OPTIONS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPrefetchOption;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.prepareTestConfiguration;

@RunWith(Parameterized.class)
public class ITestS3AContractUnbuffer extends AbstractContractUnbufferTest {

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "prefetch={0}")
  public static Collection<Object[]> params() {
    return PREFETCH_OPTIONS;
  }

  /**
   * Prefetch flag.
   */
  private final boolean prefetch;

  public ITestS3AContractUnbuffer(final boolean prefetch) {
    this.prefetch = prefetch;
  }

  /**
   * Create a configuration.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = prepareTestConfiguration(super.createConfiguration());
    disableFilesystemCaching(conf);
    return setPrefetchOption(conf, prefetch);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }
}
