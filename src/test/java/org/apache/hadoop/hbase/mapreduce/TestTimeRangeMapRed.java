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
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class TestTimeRangeMapRed extends HBaseClusterTestCase {

  private final static Log log = LogFactory.getLog(TestTimeRangeMapRed.class);

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

  protected HTableDescriptor desc;
  protected HTable table;

  public TestTimeRangeMapRed() {
    super();
    System.setProperty("hadoop.log.dir", conf.get("hadoop.log.dir"));
    conf.set("mapred.output.dir", conf.get("hadoop.tmp.dir"));
    this.setOpenMetaTable(true);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    desc = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor col = new HColumnDescriptor(FAMILY_NAME);
    col.setMaxVersions(Integer.MAX_VALUE);
    desc.addFamily(col);
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
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
      for (KeyValue kv : result.sorted()) {
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

  public void testTimeRangeMapRed()
  throws IOException, InterruptedException, ClassNotFoundException {
    prepareTest();
    runTestOnTable();
    verify();
  }

  private void prepareTest() throws IOException {
    for (Map.Entry<Long, Boolean> entry : TIMESTAMP.entrySet()) {
      Put put = new Put(KEY);
      put.add(FAMILY_NAME, COLUMN_NAME, entry.getKey(), Bytes.toBytes(false));
      table.put(put);
    }
    table.flushCommits();
  }

  private void runTestOnTable()
  throws IOException, InterruptedException, ClassNotFoundException {
    MiniMRCluster mrCluster = new MiniMRCluster(2, fs.getUri().toString(), 1);
    Job job = null;
    try {
      job = new Job(conf, "test123");
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
      mrCluster.shutdown();
      if (job != null) {
        FileUtil.fullyDelete(
          new File(job.getConfiguration().get("hadoop.tmp.dir")));
      }
    }
  }

  private void verify() throws IOException {
    Scan scan = new Scan();
    scan.addColumn(FAMILY_NAME, COLUMN_NAME);
    scan.setMaxVersions(1);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r: scanner) {
      for (KeyValue kv : r.sorted()) {
        log.debug(Bytes.toString(r.getRow()) + "\t" + Bytes.toString(kv.getFamily())
            + "\t" + Bytes.toString(kv.getQualifier())
            + "\t" + kv.getTimestamp() + "\t" + Bytes.toBoolean(kv.getValue()));
        assertEquals(TIMESTAMP.get(kv.getTimestamp()), (Boolean)Bytes.toBoolean(kv.getValue()));
      }
    }
    scanner.close();
  }

}
