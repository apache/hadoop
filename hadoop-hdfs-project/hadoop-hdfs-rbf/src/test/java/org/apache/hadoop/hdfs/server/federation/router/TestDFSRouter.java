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
package org.apache.hadoop.hdfs.server.federation.router;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.fedbalance.FedBalanceConfigs;

public class TestDFSRouter {

  @Test
  public void testDefaultConfigs() {
    Configuration configuration = DFSRouter.getConfiguration();
    String journalUri =
        configuration.get(FedBalanceConfigs.SCHEDULER_JOURNAL_URI);
    int workerThreads =
        configuration.getInt(FedBalanceConfigs.WORK_THREAD_NUM, -1);
    Assert.assertEquals("hdfs://localhost:8020/tmp/procedure", journalUri);
    Assert.assertEquals(10, workerThreads);
  }

}