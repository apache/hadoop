/*
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummy;
import org.apache.hadoop.hbase.replication.ReplicationZookeeperWrapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class TestReplicationSourceManager {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationSourceManager.class);

  private static Configuration conf;

  private static HBaseTestingUtility utility;

  private static final AtomicBoolean STOPPER = new AtomicBoolean(false);

  private static final AtomicBoolean REPLICATING = new AtomicBoolean(false);

  private static ReplicationSourceManager manager;

  private static ZooKeeperWrapper zkw;

  private static HTableDescriptor htd;

  private static HRegionInfo hri;

  private static final byte[] r1 = Bytes.toBytes("r1");

  private static final byte[] r2 = Bytes.toBytes("r2");

  private static final byte[] f1 = Bytes.toBytes("f1");

  private static final byte[] f2 = Bytes.toBytes("f2");

  private static final byte[] test = Bytes.toBytes("test");

  private static FileSystem fs;

  private static Path oldLogDir;

  private static Path logDir;


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    conf = HBaseConfiguration.create();
    conf.set("replication.replicationsource.implementation",
        ReplicationSourceDummy.class.getCanonicalName());
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    utility = new HBaseTestingUtility(conf);
    utility.startMiniZKCluster();

    zkw = ZooKeeperWrapper.createInstance(conf, "test");
    zkw.writeZNode("/hbase", "replication", "");
    zkw.writeZNode("/hbase/replication", "master",
        conf.get(HConstants.ZOOKEEPER_QUORUM)+":" +
    conf.get("hbase.zookeeper.property.clientPort")+":/1");
    zkw.writeZNode("/hbase/replication/peers", "1",
          conf.get(HConstants.ZOOKEEPER_QUORUM)+":" +
          conf.get("hbase.zookeeper.property.clientPort")+":/1");

    HRegionServer server = new HRegionServer(conf);
    ReplicationZookeeperWrapper helper = new ReplicationZookeeperWrapper(
        server.getZooKeeperWrapper(), conf,
        REPLICATING, "123456789");
    fs = FileSystem.get(conf);
    oldLogDir = new Path(utility.getTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(utility.getTestDir(),
        HConstants.HREGION_LOGDIR_NAME);

    manager = new ReplicationSourceManager(helper,
        conf, STOPPER, fs, REPLICATING, logDir, oldLogDir);
    manager.addSource("1");

    htd = new HTableDescriptor(test);
    HColumnDescriptor col = new HColumnDescriptor("f1");
    col.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    htd.addFamily(col);
    col = new HColumnDescriptor("f2");
    col.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
    htd.addFamily(col);

    hri = new HRegionInfo(htd, r1, r2);


  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    manager.join();
    utility.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    fs.delete(logDir, true);
    fs.delete(oldLogDir, true);
  }

  @After
  public void tearDown() throws Exception {
    setUp();
  }

  @Test
  public void testLogRoll() throws Exception {
    long seq = 0;
    long baseline = 1000;
    long time = baseline;
    KeyValue kv = new KeyValue(r1, f1, r1);
    WALEdit edit = new WALEdit();
    edit.add(kv);

    HLog hlog = new HLog(fs, logDir, oldLogDir, conf, null, manager,
      URLEncoder.encode("regionserver:60020", "UTF8"));

    manager.init();

    // Testing normal log rolling every 20
    for(long i = 1; i < 101; i++) {
      if(i > 1 && i % 20 == 0) {
        hlog.rollWriter();
      }
      LOG.info(i);
      HLogKey key = new HLogKey(hri.getRegionName(),
        test, seq++, System.currentTimeMillis());
      hlog.append(hri, key, edit);
    }

    // Simulate a rapid insert that's followed
    // by a report that's still not totally complete (missing last one)
    LOG.info(baseline + " and " + time);
    baseline += 101;
    time = baseline;
    LOG.info(baseline + " and " + time);

    for (int i = 0; i < 3; i++) {
      HLogKey key = new HLogKey(hri.getRegionName(),
        test, seq++, System.currentTimeMillis());
      hlog.append(hri, key, edit);
    }

    assertEquals(6, manager.getHLogs().size());

    hlog.rollWriter();

    manager.logPositionAndCleanOldLogs(manager.getSources().get(0).getCurrentPath(),
        "1", 0, false);

    HLogKey key = new HLogKey(hri.getRegionName(),
          test, seq++, System.currentTimeMillis());
    hlog.append(hri, key, edit);

    assertEquals(1, manager.getHLogs().size());


    // TODO Need a case with only 2 HLogs and we only want to delete the first one
  }

}
