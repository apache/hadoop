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
package org.apache.hadoop.yarn.server.timeline;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.iq80.leveldb.DB;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test class for verification of RollingLevelDB. */
public class TestRollingLevelDB {
  private Configuration conf = new YarnConfiguration();
  private FileSystem lfs;
  private MyRollingLevelDB rollingLevelDB;

  /** RollingLevelDB for testing that has a setting current time. */
  public static class MyRollingLevelDB extends RollingLevelDB {
    private long currentTimeMillis;

    MyRollingLevelDB() {
      super("Test");
      this.currentTimeMillis = System.currentTimeMillis();
    }

    @Override
    protected long currentTimeMillis() {
      return currentTimeMillis;
    }

    public void setCurrentTimeMillis(long time) {
      this.currentTimeMillis = time;
    }
  };

  @Before
  public void setup() throws Exception {
    lfs = FileSystem.getLocal(conf);
    File fsPath = new File("target", this.getClass().getSimpleName() +
        "-tmpDir").getAbsoluteFile();
    conf.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH,
        fsPath.getAbsolutePath());
    lfs.delete(new Path(fsPath.getAbsolutePath()), true);
    rollingLevelDB = new MyRollingLevelDB();
  }

  @Test
  public void testInsertAfterRollPeriodRollsDB() throws Exception {

    rollingLevelDB.init(conf);
    long now = rollingLevelDB.currentTimeMillis();
    DB db = rollingLevelDB.getDBForStartTime(now);
    long startTime = rollingLevelDB.getStartTimeFor(db);
    Assert.assertEquals("Received level db for incorrect start time",
        rollingLevelDB.computeCurrentCheckMillis(now),
        startTime);
    now = rollingLevelDB.getNextRollingTimeMillis();
    rollingLevelDB.setCurrentTimeMillis(now);
    db = rollingLevelDB.getDBForStartTime(now);
    startTime = rollingLevelDB.getStartTimeFor(db);
    Assert.assertEquals("Received level db for incorrect start time",
        rollingLevelDB.computeCurrentCheckMillis(now),
        startTime);
  }

  @Test
  public void testInsertForPreviousPeriodAfterRollPeriodRollsDB()
      throws Exception {

    rollingLevelDB.init(conf);
    long now = rollingLevelDB.currentTimeMillis();
    now = rollingLevelDB.computeCurrentCheckMillis(now);
    rollingLevelDB.setCurrentTimeMillis(now);
    DB db = rollingLevelDB.getDBForStartTime(now - 1);
    long startTime = rollingLevelDB.getStartTimeFor(db);
    Assert.assertEquals("Received level db for incorrect start time",
        rollingLevelDB.computeCurrentCheckMillis(now - 1),
        startTime);
  }
}
