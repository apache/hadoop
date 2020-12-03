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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;

import org.junit.Assert;
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
  public void testTops() throws Exception {
    long time = WINDOW_LEN_MS + BUCKET_LEN * 3 / 2;
    for (int i = 0; i < users.length; i++)
      manager.recordMetric(time, "open", users[i], (i + 1) * 2);
    time++;
    for (int i = 0; i < users.length; i++)
      manager.recordMetric(time, "close", users[i], i + 1);
    time++;
    TopWindow tops = manager.snapshot(time);

    assertEquals("Unexpected number of ops", 3, tops.getOps().size());
    assertEquals(TopConf.ALL_CMDS, tops.getOps().get(0).getOpType());
    for (Op op : tops.getOps()) {
      final List<User> topUsers = op.getTopUsers();
      assertEquals("Unexpected number of users", N_TOP_USERS, topUsers.size());
      if (op.getOpType().equals("open")) {
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
    assertEquals("Unexpected number of ops", 2, tops.getOps().size());
    assertEquals(TopConf.ALL_CMDS, tops.getOps().get(0).getOpType());
    final Op op = tops.getOps().get(1);
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

  @Test
  public void windowReset() throws Exception {
    Configuration config = new Configuration();
    config.setInt(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY, 1);
    config.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, N_TOP_USERS);
    int period = 2;
    RollingWindowManager rollingWindowManager =
        new RollingWindowManager(config, period);
    rollingWindowManager.recordMetric(0, "op1", users[0], 3);
    checkValues(rollingWindowManager, 0, "op1", 3, 3);
    checkValues(rollingWindowManager, period - 1, "op1", 3, 3);
    checkValues(rollingWindowManager, period, "op1", 0, 0);
  }

  @Test
  public void testTotal() throws Exception {
    Configuration config = new Configuration();
    config.setInt(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY, 1);
    config.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, N_TOP_USERS);
    int period = 10;
    RollingWindowManager rollingWindowManager =
        new RollingWindowManager(config, period);

    long t = 0;
    rollingWindowManager.recordMetric(t, "op1", users[0], 3);
    checkValues(rollingWindowManager, t, "op1", 3, 3);

    // both should have a value.
    t = (long)(period * .5);
    rollingWindowManager.recordMetric(t, "op2", users[0], 4);
    checkValues(rollingWindowManager, t, "op1", 3, 7);
    checkValues(rollingWindowManager, t, "op2", 4, 7);

    // neither should reset.
    t = period - 1;
    checkValues(rollingWindowManager, t, "op1", 3, 7);
    checkValues(rollingWindowManager, t, "op2", 4, 7);

    // op1 should reset in its next period, but not op2.
    t = period;
    rollingWindowManager.recordMetric(10, "op1", users[0], 10);
    checkValues(rollingWindowManager, t, "op1", 10, 14);
    checkValues(rollingWindowManager, t, "op2", 4, 14);

    // neither should reset.
    t = (long)(period * 1.25);
    rollingWindowManager.recordMetric(t, "op2", users[0], 7);
    checkValues(rollingWindowManager, t, "op1", 10, 21);
    checkValues(rollingWindowManager, t, "op2", 11, 21);

    // op2 should reset.
    t = (long)(period * 1.5);
    rollingWindowManager.recordMetric(t, "op2", users[0], 13);
    checkValues(rollingWindowManager, t, "op1", 10, 23);
    checkValues(rollingWindowManager, t, "op2", 13, 23);
  }

  @Test
  public void testWithFuzzing() throws Exception {
    Configuration config = new Configuration();
    config.setInt(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY, 1);
    config.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, N_TOP_USERS);
    int period = users.length/2;
    RollingWindowManager rollingWindowManager =
        new RollingWindowManager(config, period);

    String[] ops = {"op1", "op2", "op3", "op4"};
    Random rand = new Random();
    for (int i=0; i < 10000; i++) {
      rollingWindowManager.recordMetric(i, ops[rand.nextInt(ops.length)],
          users[rand.nextInt(users.length)],
          rand.nextInt(100));
      TopWindow window = rollingWindowManager.snapshot(i);
      checkTotal(window);
    }
  }

  @Test
  public void testOpTotal() throws Exception {
    int numTopUsers = 2;
    Configuration config = new Configuration();
    config.setInt(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY, 1);
    config.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, numTopUsers);
    int period = users.length/2;
    RollingWindowManager rollingWindowManager =
        new RollingWindowManager(config, period);

    int numOps = 3;
    rollingWindowManager.recordMetric(0, "op1", "user1", 10);
    rollingWindowManager.recordMetric(0, "op1", "user2", 20);
    rollingWindowManager.recordMetric(0, "op1", "user3", 30);

    rollingWindowManager.recordMetric(0, "op2", "user1", 1);
    rollingWindowManager.recordMetric(0, "op2", "user4", 40);
    rollingWindowManager.recordMetric(0, "op2", "user5", 50);

    rollingWindowManager.recordMetric(0, "op3", "user6", 1);
    rollingWindowManager.recordMetric(0, "op3", "user7", 11);
    rollingWindowManager.recordMetric(0, "op3", "user8", 1);

    TopWindow window = rollingWindowManager.snapshot(0);
    Assert.assertEquals(numOps + 1, window.getOps().size());

    Op allOp = window.getOps().get(0);
    Assert.assertEquals(TopConf.ALL_CMDS, allOp.getOpType());
    List<User> topUsers = allOp.getTopUsers();
    Assert.assertEquals(numTopUsers * numOps, topUsers.size());
    // ensure all the top users for each op are present in the total op.
    for (int i = 1; i < numOps; i++) {
      Assert.assertTrue(
          topUsers.containsAll(window.getOps().get(i).getTopUsers()));
    }
  }

  private void checkValues(RollingWindowManager rwManager, long time,
      String opType, long value, long expectedTotal) throws Exception {
    TopWindow window = rwManager.snapshot(time);
    for (Op windowOp : window.getOps()) {
      if (opType.equals(windowOp.getOpType())) {
        assertEquals(value, windowOp.getTotalCount());
        break;
      }
    }
    assertEquals(expectedTotal, checkTotal(window));
  }

  private long checkTotal(TopWindow window) throws Exception {
    long allOpTotal = 0;
    long computedOpTotal = 0;

    Map<String, User> userOpTally = new HashMap<>();
    for (String user : users) {
      userOpTally.put(user, new User(user, 0));
    }
    for (Op windowOp : window.getOps()) {
      int multiplier;
      if (TopConf.ALL_CMDS.equals(windowOp.getOpType())) {
        multiplier = -1;
        allOpTotal += windowOp.getTotalCount();
      } else {
        multiplier = 1;
        computedOpTotal += windowOp.getTotalCount();
      }
      for (User user : windowOp.getAllUsers()) {
        userOpTally.get(user.getUser()).add((int)(multiplier*user.getCount()));
      }
    }
    assertEquals(allOpTotal, computedOpTotal);
    for (String user : userOpTally.keySet()) {
      assertEquals(0, userOpTally.get(user).getCount());
    }
    return computedOpTotal;
  }
}
