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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test extends TestReconstructStripedFile to use a random
 * (non-default) EC policy.
 */
public class TestReconstructStripedFileWithRandomECPolicy extends
    TestReconstructStripedFile {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestReconstructStripedFileWithRandomECPolicy.class);

  private ErasureCodingPolicy ecPolicy;

  public TestReconstructStripedFileWithRandomECPolicy() {
    // If you want to debug this test with a specific ec policy, please use
    // SystemErasureCodingPolicies class.
    // e.g. ecPolicy = SystemErasureCodingPolicies.getByID(RS_3_2_POLICY_ID);
    ecPolicy = StripedFileTestUtil.getRandomNonDefaultECPolicy();
    LOG.info("run {} with {}.",
        TestReconstructStripedFileWithRandomECPolicy.class
            .getSuperclass().getSimpleName(), ecPolicy.getName());
  }

  @Override
  public ErasureCodingPolicy getEcPolicy() {
    return ecPolicy;
  }
}
