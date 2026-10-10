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

import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.FORCE_CLOSE_SENTINEL_PATH;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TIME_WINDOW_SENTINEL_PATH;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TrashOption;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.WORK_THREAD_NUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFedBalance {
  @Test
  public void testLoadFedBalanceDefaultConf() {
    Configuration conf = FedBalance.getDefaultConf();
    assertNotNull(conf.get(SCHEDULER_JOURNAL_URI));
    assertNotNull(conf.get(WORK_THREAD_NUM));
    assertEquals("", conf.getTrimmed(TIME_WINDOW_SENTINEL_PATH, ""));
    assertEquals("", conf.getTrimmed(FORCE_CLOSE_SENTINEL_PATH, ""));
  }

  @Test
  public void testFedBalanceOptionsRegistered() {
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.STOP_AFTER_INITIAL_COPY.getOpt()));
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.START_FROM_INCREMENTAL.getOpt()));
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.STOP_ON_SMALL_DIFF.getOpt()));
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.TIME_WINDOW_SENTINEL.getOpt()));
    assertTrue(FedBalanceOptions.CLI_OPTIONS.hasOption(
        FedBalanceOptions.FORCE_CLOSE_SENTINEL.getOpt()));
  }

  @Test
  public void testFedBalanceContextOperationalControlSerialization()
      throws IOException {
    Configuration conf = new Configuration(false);
    FedBalanceContext context = new FedBalanceContext.Builder(
        new Path("hdfs://src.example.com/src"),
        new Path("hdfs://dst.example.com/dst"), FedBalance.NO_MOUNT, conf)
        .setMapNum(10)
        .setBandwidthLimit(1)
        .setTrash(TrashOption.TRASH)
        .setDelayDuration(1000)
        .setStopAfterInitialCopy(true)
        .setStartFromIncremental(true)
        .setStopOnSmallDiff(true)
        .setTimeWindowSentinelPath("hdfs://nn/sentinel/time")
        .setForceCloseSentinelPath("hdfs://nn/sentinel/force")
        .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    context.write(new DataOutputStream(out));

    FedBalanceContext recovered = new FedBalanceContext();
    recovered.readFields(new DataInputStream(
        new ByteArrayInputStream(out.toByteArray())));

    assertTrue(recovered.getStopAfterInitialCopy());
    assertTrue(recovered.getStartFromIncremental());
    assertTrue(recovered.getStopOnSmallDiff());
    assertEquals("hdfs://nn/sentinel/time",
        recovered.getTimeWindowSentinelPath());
    assertEquals("hdfs://nn/sentinel/force",
        recovered.getForceCloseSentinelPath());
  }
}
