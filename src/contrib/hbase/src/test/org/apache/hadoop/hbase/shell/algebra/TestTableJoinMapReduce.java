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
package org.apache.hadoop.hbase.shell.algebra;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.StaticTestEnvironment;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MultiRegionTable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 * HBase shell join test
 */
public class TestTableJoinMapReduce extends MultiRegionTable {
  @SuppressWarnings("hiding")
  private static final Log LOG = LogFactory.getLog(TestTableJoinMapReduce.class
      .getName());
  static final String FIRST_RELATION = "r1";
  static final String SECOND_RELATION = "r2";
  static final String JOIN_EXPRESSION = "r1.c: = r2.ROW BOOL ";
  static final String FIRST_COLUMNS = "a: b: c:";
  static final String SECOND_COLUMNS = "d: e:";
  static final String OUTPUT_TABLE = "result_table";
  private MiniDFSCluster dfsCluster = null;
  private FileSystem fs;
  private Path dir;
  private MiniHBaseCluster hCluster = null;

  /**
   * {@inheritDoc}
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dfsCluster = new MiniDFSCluster(conf, 1, true, (String[]) null);
    try {
      fs = dfsCluster.getFileSystem();
      dir = new Path("/hbase");
      fs.mkdirs(dir);
      // Start up HBase cluster
      hCluster = new MiniHBaseCluster(conf, 1, dfsCluster);
    } catch (Exception e) {
      StaticTestEnvironment.shutdownDfs(dfsCluster);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (hCluster != null) {
      hCluster.shutdown();
    }
    StaticTestEnvironment.shutdownDfs(dfsCluster);
  }

  public void testTableJoinMapReduce() {
    HTable table = null;
    try {
      HTableDescriptor desc = new HTableDescriptor(FIRST_RELATION);
      String[] columns = FIRST_COLUMNS.split(" ");
      for (int i = 0; i < columns.length; i++) {
        desc.addFamily(new HColumnDescriptor(columns[i]));
      }
      HBaseAdmin admin = new HBaseAdmin(this.conf);
      admin.createTable(desc);

      // insert random data into the input table
      table = new HTable(conf, new Text(FIRST_RELATION));
      for (int j = 0; j < 5; j++) {
        long lockid = table.startUpdate(new Text("rowKey" + j));
        table.put(lockid, new Text("a:"), Integer.toString(j).getBytes(
            HConstants.UTF8_ENCODING));
        table.put(lockid, new Text("b:"), Integer.toString(j).getBytes(
            HConstants.UTF8_ENCODING));
        table.put(lockid, new Text("c:"), ("joinKey-" + Integer.toString(j))
            .getBytes(HConstants.UTF8_ENCODING));
        table.commit(lockid, System.currentTimeMillis());
      }

    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (table != null) {
        table.close();
      }
    }

    try {
      HTableDescriptor desc = new HTableDescriptor(SECOND_RELATION);
      String[] columns = SECOND_COLUMNS.split(" ");
      for (int i = 0; i < columns.length; i++) {
        desc.addFamily(new HColumnDescriptor(columns[i]));
      }
      HBaseAdmin admin = new HBaseAdmin(this.conf);
      admin.createTable(desc);

      // insert random data into the input table
      table = new HTable(conf, new Text(SECOND_RELATION));
      for (int j = 0; j < 3; j++) {
        long lockid = table.startUpdate(new Text("joinKey-" + j));
        table.put(lockid, new Text("d:"), ("s-" + Integer.toString(j))
            .getBytes(HConstants.UTF8_ENCODING));
        table.put(lockid, new Text("e:"), ("s-" + Integer.toString(j))
            .getBytes(HConstants.UTF8_ENCODING));
        table.commit(lockid, System.currentTimeMillis());
      }

    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (table != null) {
        table.close();
      }
    }

    try {
      HTableDescriptor output = new HTableDescriptor(OUTPUT_TABLE);
      String[] columns = (FIRST_COLUMNS + " " + SECOND_COLUMNS).split(" ");
      for (int i = 0; i < columns.length; i++) {
        output.addFamily(new HColumnDescriptor(columns[i]));
      }
      // create output table
      HBaseAdmin admin = new HBaseAdmin(this.conf);
      admin.createTable(output);
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    MiniMRCluster mrCluster = null;
    try {
      mrCluster = new MiniMRCluster(2, fs.getUri().toString(), 1);

      JobConf jobConf = new JobConf(conf, TestTableJoinMapReduce.class);
      jobConf.setJobName("process table join mapreduce");
      jobConf.setNumMapTasks(2);
      jobConf.setNumReduceTasks(1);

      IndexJoinMap.initJob(FIRST_RELATION, SECOND_RELATION, FIRST_COLUMNS,
          SECOND_COLUMNS, JOIN_EXPRESSION, IndexJoinMap.class, jobConf);
      TableReduce.initJob(OUTPUT_TABLE, IndexJoinReduce.class, jobConf);

      JobClient.runJob(jobConf);

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      mrCluster.shutdown();
    }

    try {
    verify(conf, OUTPUT_TABLE);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Check the result table lattice size.
   * 
   * @param conf
   * @param outputTable
   * @throws IOException
   */
  private void verify(HBaseConfiguration conf, String outputTable)
      throws IOException {
    HTable table = new HTable(conf, new Text(outputTable));
    Text[] columns = { new Text("a:"), new Text("b:"), new Text("c:"),
        new Text("d:"), new Text("e:") };
    HScannerInterface scanner = table.obtainScanner(columns,
        HConstants.EMPTY_START_ROW);

    try {
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();

      int i = 0;
      while (scanner.next(key, results)) {
        assertTrue(results.keySet().size() == 5);
        LOG.info("result_table.column.size: " + results.keySet().size());
        i++;
      }
      assertTrue(i == 3);
      LOG.info("result_table.row.count: " + i);
    } finally {
      scanner.close();
    }

  }
}
