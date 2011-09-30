/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimeRangeMapRed {
  private final static Log log = LogFactory.getLog(TestTimeRangeMapRed.class);
  private static final HBaseTestingUtility UTIL =
    new HBaseTestingUtility();
  private HBaseAdmin admin;

  private static final byte [] KEY = Bytes.toBytes("row1");
  private static final NavigableMap<Long, Boolean> TIMESTAMP =
    new TreeMap<Long, Boolean>();
  static {
    TIMESTAMP.put((long)1245620000, false);
    TIMESTAMP.put((long)1245620005, true); // include
    TIMESTAMP.put((long)1245620010, true); // include
    TIMESTAMP.put((long)1245620055, true); // include
    TIMESTAMP.put((long)1245620100, true); // include
    TIMESTAMP.put((long)1245620150, false);
    TIMESTAMP.put((long)1245620250, false);
  }
  static final long MINSTAMP = 1245620005;
  static final long MAXSTAMP = 1245620100 + 1; // maxStamp itself is excluded. so increment it.

  static final byte[] TABLE_NAME = Bytes.toBytes("table123");
  static final byte[] FAMILY_NAME = Bytes.toBytes("text");
  static final byte[] COLUMN_NAME = Bytes.toBytes("input");

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("hadoop.log.dir",
      UTIL.getConfiguration().get("hadoop.log.dir"));
    UTIL.getConfiguration().set("mapred.output.dir",
      UTIL.getConfiguration().get("hadoop.tmp.dir"));
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws MasterNotRunningException, ZooKeeperConnectionException {
    this.admin = new HBaseAdmin(UTIL.getConfiguration());
  }

  @After
  public void after() throws IOException {
    this.admin.close();
  }

  private static class ProcessTimeRangeMapper
  extends TableMapper<ImmutableBytesWritable, MapWritable>
  implements Configurable {

    private Configuration conf = null;
    private HTable table = null;

    @Override
    public void map(ImmutableBytesWritable key, Result result,
        Context context)
    throws IOException {
      List<Long> tsList = new ArrayList<Long>();
      for (KeyValue kv : result.list()) {
        tsList.add(kv.getTimestamp());
      }

      for (Long ts : tsList) {
        Put put = new Put(key.get());
        put.add(FAMILY_NAME, COLUMN_NAME, ts, Bytes.toBytes(true));
        table.put(put);
      }
      table.flushCommits();
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
      this.conf = configuration;
      try {
        table = new HTable(HBaseConfiguration.create(conf), TABLE_NAME);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testTimeRangeMapRed()
  throws IOException, InterruptedException, ClassNotFoundException {
    final HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    final HColumnDescriptor col = new HColumnDescriptor(FAMILY_NAME);
    col.setMaxVersions(Integer.MAX_VALUE);
    desc.addFamily(col);
    admin.createTable(desc);
    HTable table = new HTable(UTIL.getConfiguration(), desc.getName());
    prepareTest(table);
    runTestOnTable();
    verify(table);
  }

  private void prepareTest(final HTable table) throws IOException {
    for (Map.Entry<Long, Boolean> entry : TIMESTAMP.entrySet()) {
      Put put = new Put(KEY);
      put.add(FAMILY_NAME, COLUMN_NAME, entry.getKey(), Bytes.toBytes(false));
      table.put(put);
    }
    table.flushCommits();
  }

  private void runTestOnTable()
  throws IOException, InterruptedException, ClassNotFoundException {
    UTIL.startMiniMapReduceCluster(1);
    Job job = null;
    try {
      job = new Job(UTIL.getConfiguration(), "test123");
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setNumReduceTasks(0);
      Scan scan = new Scan();
      scan.addColumn(FAMILY_NAME, COLUMN_NAME);
      scan.setTimeRange(MINSTAMP, MAXSTAMP);
      scan.setMaxVersions();
      TableMapReduceUtil.initTableMapperJob(Bytes.toString(TABLE_NAME),
        scan, ProcessTimeRangeMapper.class, Text.class, Text.class, job);
      job.waitForCompletion(true);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      UTIL.shutdownMiniMapReduceCluster();
      if (job != null) {
        FileUtil.fullyDelete(
          new File(job.getConfiguration().get("hadoop.tmp.dir")));
      }
    }
  }

  private void verify(final HTable table) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(FAMILY_NAME, COLUMN_NAME);
    scan.setMaxVersions(1);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r: scanner) {
      for (KeyValue kv : r.list()) {
        log.debug(Bytes.toString(r.getRow()) + "\t" + Bytes.toString(kv.getFamily())
            + "\t" + Bytes.toString(kv.getQualifier())
            + "\t" + kv.getTimestamp() + "\t" + Bytes.toBoolean(kv.getValue()));
        org.junit.Assert.assertEquals(TIMESTAMP.get(kv.getTimestamp()),
          (Boolean)Bytes.toBoolean(kv.getValue()));
      }
    }
    scanner.close();
  }
}