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

package org.apache.hadoop.applications.mawo.server.common;


import org.junit.Assert;
import org.junit.Test;

/**
 * Test MaWo configuration.
 */
public class TestMaWoConfiguration {

  /**
   * Validate default MaWo Configurations.
   */
  @Test
  public void testMaWoConfiguration() {

    MawoConfiguration mawoConf = new MawoConfiguration();

    // validate Rpc server port
    Assert.assertEquals(mawoConf.getRpcServerPort(), 5120);

    // validate Rpc hostname
    Assert.assertTrue("localhost".equals(mawoConf.getRpcHostName()));

    // validate job queue storage conf
    boolean jobQueueStorage = mawoConf.getJobQueueStorageEnabled();
    Assert.assertTrue(jobQueueStorage);

    // validate default teardownWorkerValidity Interval
    Assert.assertEquals(mawoConf.getTeardownWorkerValidityInterval(), 120000);

    // validate Zk related configs
    Assert.assertTrue("/tmp/mawoRoot".equals(mawoConf.getZKParentPath()));
    Assert.assertTrue("localhost:2181".equals(mawoConf.getZKAddress()));
    Assert.assertEquals(1000, mawoConf.getZKRetryIntervalMS());
    Assert.assertEquals(10000, mawoConf.getZKSessionTimeoutMS());
    Assert.assertEquals(1000, mawoConf.getZKRetriesNum());
  }


}
