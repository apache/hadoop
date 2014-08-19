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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.junit.Test;

public class TestBalancerWithSaslDataTransfer extends SaslDataTransferTestCase {

  private static final TestBalancer TEST_BALANCER = new TestBalancer();

  @Test
  public void testBalancer0Authentication() throws Exception {
    TEST_BALANCER.testBalancer0Internal(createSecureConfig("authentication"));
  }

  @Test
  public void testBalancer0Integrity() throws Exception {
    TEST_BALANCER.testBalancer0Internal(createSecureConfig("integrity"));
  }

  @Test
  public void testBalancer0Privacy() throws Exception {
    TEST_BALANCER.testBalancer0Internal(createSecureConfig("privacy"));
  }
}
