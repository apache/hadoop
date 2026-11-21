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

import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.VERIFY_ENABLED;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TrashOption;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.WORK_THREAD_NUM;

public class TestFedBalance {
  @Test
  public void testLoadFedBalanceDefaultConf() {
    Configuration conf = FedBalance.getDefaultConf();
    assertNotNull(conf.get(SCHEDULER_JOURNAL_URI));
    assertNotNull(conf.get(WORK_THREAD_NUM));
    assertFalse(conf.getBoolean(VERIFY_ENABLED, true));
  }

  @Test
  public void testFedBalanceOptionsRegistered() {
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.VERIFY.getOpt()));
  }

  @Test
  public void testFedBalanceContextVerifySerialization() throws IOException {
    Configuration conf = new Configuration(false);
    FedBalanceContext context = new FedBalanceContext.Builder(
        new Path("hdfs://src.example.com/src"),
        new Path("hdfs://dst.example.com/dst"), FedBalance.NO_MOUNT, conf)
        .setMapNum(10)
        .setBandwidthLimit(1)
        .setTrash(TrashOption.TRASH)
        .setDelayDuration(1000)
        .setVerify(true)
        .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    context.write(new DataOutputStream(out));

    FedBalanceContext recovered = new FedBalanceContext();
    recovered.readFields(new DataInputStream(
        new ByteArrayInputStream(out.toByteArray())));

    assertTrue(recovered.getVerify());
  }
}
