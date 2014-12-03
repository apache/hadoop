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
package org.apache.hadoop.hdfs.server.namenode.top.window;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.MetricValueMap;

public class TestRollingWindowManager {

  Configuration conf;
  RollingWindowManager manager;
  String[] users;
  final static int MIN_2_MS = 60000;

  final int WINDOW_LEN_MS = 1 * MIN_2_MS;
  final int BUCKET_CNT = 10;
  final int N_TOP_USERS = 10;
  final int BUCKET_LEN = WINDOW_LEN_MS / BUCKET_CNT;

  @Before
  public void init() {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY, BUCKET_CNT);
    conf.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, N_TOP_USERS);
    manager = new RollingWindowManager(conf, WINDOW_LEN_MS);
    users = new String[2 * N_TOP_USERS];
    for (int i = 0; i < users.length; i++) {
      users[i] = "user" + i;
    }
  }

  @Test
  public void testTops() {
    long time = WINDOW_LEN_MS + BUCKET_LEN * 3 / 2;
    for (int i = 0; i < users.length; i++)
      manager.recordMetric(time, "open", users[i], (i + 1) * 2);
    time++;
    for (int i = 0; i < users.length; i++)
      manager.recordMetric(time, "close", users[i], i + 1);
    time++;
    MetricValueMap tops = manager.snapshot(time);

    assertEquals("The number of returned top metrics is invalid",
        2 * (N_TOP_USERS + 1), tops.size());
    int userIndex = users.length - 2;
    String metricName = RollingWindowManager.createMetricName("open",
        users[userIndex]);
    boolean includes = tops.containsKey(metricName);
    assertTrue("The order of entries in top metrics is wrong", includes);
    assertEquals("The reported value by top is different from recorded one",
        (userIndex + 1) * 2, ((Long) tops.get(metricName)).longValue());

    // move the window forward not to see the "open" results
    time += WINDOW_LEN_MS - 2;
    // top should not include only "close" results
    tops = manager.snapshot(time);
    assertEquals("The number of returned top metrics is invalid",
        N_TOP_USERS + 1, tops.size());
    includes = tops.containsKey(metricName);
    assertFalse("After rolling, the top list still includes the stale metrics",
        includes);

    metricName = RollingWindowManager.createMetricName("close",
        users[userIndex]);
    includes = tops.containsKey(metricName);
    assertTrue("The order of entries in top metrics is wrong", includes);
    assertEquals("The reported value by top is different from recorded one",
        (userIndex + 1), ((Long) tops.get(metricName)).longValue());
  }
}
