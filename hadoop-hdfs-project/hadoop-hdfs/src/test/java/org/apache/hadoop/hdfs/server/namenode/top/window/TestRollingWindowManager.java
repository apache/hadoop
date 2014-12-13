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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.Op;
import static org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.TopWindow;
import static org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.User;
import static org.junit.Assert.assertEquals;

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
    TopWindow tops = manager.snapshot(time);

    assertEquals("Unexpected number of ops", 2, tops.getOps().size());
    for (Op op : tops.getOps()) {
      final List<User> topUsers = op.getTopUsers();
      assertEquals("Unexpected number of users", N_TOP_USERS, topUsers.size());
      if (op.getOpType() == "open") {
        for (int i = 0; i < topUsers.size(); i++) {
          User user = topUsers.get(i);
          assertEquals("Unexpected count for user " + user.getUser(),
              (users.length-i)*2, user.getCount());
        }
        // Closed form of sum(range(2,42,2))
        assertEquals("Unexpected total count for op", 
            (2+(users.length*2))*(users.length/2),
            op.getTotalCount());
      }
    }

    // move the window forward not to see the "open" results
    time += WINDOW_LEN_MS - 2;
    tops = manager.snapshot(time);
    assertEquals("Unexpected number of ops", 1, tops.getOps().size());
    final Op op = tops.getOps().get(0);
    assertEquals("Should only see close ops", "close", op.getOpType());
    final List<User> topUsers = op.getTopUsers();
    for (int i = 0; i < topUsers.size(); i++) {
      User user = topUsers.get(i);
      assertEquals("Unexpected count for user " + user.getUser(),
          (users.length-i), user.getCount());
    }
    // Closed form of sum(range(1,21))
    assertEquals("Unexpected total count for op",
        (1 + users.length) * (users.length / 2), op.getTotalCount());
  }
}
