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
package org.apache.hadoop.ha;

import java.util.Arrays;

import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;

public abstract class ActiveStandbyElectorTestUtil {

  public static void waitForActiveLockData(TestContext ctx,
      ZooKeeperServer zks, String parentDir, byte[] activeData)
      throws Exception {
    while (true) {
      if (ctx != null) {
        ctx.checkException();
      }
      try {
        Stat stat = new Stat();
        byte[] data = zks.getZKDatabase().getData(
          parentDir + "/" +
          ActiveStandbyElector.LOCK_FILENAME, stat, null);
        if (activeData != null &&
            Arrays.equals(activeData, data)) {
          return;
        }
      } catch (NoNodeException nne) {
        if (activeData == null) {
          return;
        }
      }
      Thread.sleep(50);
    }
  }

  public static void waitForElectorState(TestContext ctx,
      ActiveStandbyElector elector,
      ActiveStandbyElector.State state) throws Exception { 
    while (elector.getStateForTests() != state) {
      if (ctx != null) {
        ctx.checkException();
      }
      Thread.sleep(50);
    }
  }
}
