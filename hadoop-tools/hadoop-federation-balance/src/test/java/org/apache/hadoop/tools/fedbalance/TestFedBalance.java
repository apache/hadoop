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
package org.apache.hadoop.tools.fedbalance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.PRESERVE_ACL_ENABLED;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TrashOption;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.WORK_THREAD_NUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFedBalance {
  @Test
  public void testLoadFedBalanceDefaultConf() {
    Configuration conf = FedBalance.getDefaultConf();
    assertNotNull(conf.get(SCHEDULER_JOURNAL_URI));
    assertNotNull(conf.get(WORK_THREAD_NUM));
    assertTrue(conf.getBoolean(PRESERVE_ACL_ENABLED, false));
  }

  @Test
  public void testFedBalanceOptionsRegistered() {
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.DIFF_THRESHOLD.getOpt()));
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.SKIP_ACL_PRESERVE.getOpt()));
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.PRESERVE_TIMES.getOpt()));
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.DISTCP_STRATEGY.getOpt()));
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.LIST_STATUS_THREADS.getOpt()));
  }

  @Test
  public void testFedBalanceContextDistCpOptionSerialization()
      throws IOException {
    Configuration conf = new Configuration(false);
    FedBalanceContext context = new FedBalanceContext.Builder(
        new Path("hdfs://src/data"), new Path("hdfs://dst/data"),
        "mount", conf)
        .setMapNum(1)
        .setBandwidthLimit(1)
        .setTrash(TrashOption.SKIP)
        .setDelayDuration(1)
        .setDiffThreshold(3)
        .setPreserveAcl(false)
        .setPreserveTimes(true)
        .setDistCpStrategy("dynamic")
        .setNumListstatusThreads(8)
        .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    context.write(new DataOutputStream(out));

    FedBalanceContext recovered = new FedBalanceContext();
    recovered.readFields(new DataInputStream(
        new ByteArrayInputStream(out.toByteArray())));

    assertEquals(3, recovered.getDiffThreshold());
    assertFalse(recovered.getPreserveAcl());
    assertTrue(recovered.getPreserveTimes());
    assertEquals("dynamic", recovered.getDistCpStrategy());
    assertEquals(8, recovered.getNumListstatusThreads());
  }
}
