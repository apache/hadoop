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
package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestReplicationSource {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationSource.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static FileSystem fs;
  private static Path oldLogDir;
  private static Path logDir;
  private static Configuration conf = HBaseConfiguration.create();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniDFSCluster(1);
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    oldLogDir = new Path(fs.getHomeDirectory(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(fs.getHomeDirectory(),
        HConstants.HREGION_LOGDIR_NAME);
  }

  /**
   * Sanity check that we can move logs around while we are reading
   * from them. Should this test fail, ReplicationSource would have a hard
   * time reading logs that are being archived.
   * @throws Exception
   */
  @Test
  public void testLogMoving() throws Exception{
    Path logPath = new Path(logDir, "log");
    HLog.Writer writer = HLog.createWriter(fs, logPath, conf);
    for(int i = 0; i < 3; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      HLogKey key = new HLogKey(b, b, 0, 0, HConstants.DEFAULT_CLUSTER_ID);
      writer.append(new HLog.Entry(key, edit));
      writer.sync();
    }
    writer.close();

    HLog.Reader reader = HLog.getReader(fs, logPath, conf);
    HLog.Entry entry = reader.next();
    assertNotNull(entry);

    Path oldLogPath = new Path(oldLogDir, "log");
    fs.rename(logPath, oldLogPath);

    entry = reader.next();
    assertNotNull(entry);

    entry = reader.next();
    entry = reader.next();

    assertNull(entry);

  }
}
