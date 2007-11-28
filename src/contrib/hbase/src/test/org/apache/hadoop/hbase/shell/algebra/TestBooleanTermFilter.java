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
import java.util.Map;
import java.util.Random;
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
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MultiRegionTable;
import org.apache.hadoop.hbase.mapred.IdentityTableReduce;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

public class TestBooleanTermFilter extends MultiRegionTable {
  @SuppressWarnings("hiding")
  private static final Log LOG = LogFactory.getLog(TestBooleanTermFilter.class
      .getName());

  static final String INPUT_TABLE = "test_table";
  static final String OUTPUT_TABLE = "result_table";
  static final Text COUNT_COLUMNFAMILY = new Text("count:");
  static final Text RANDOMINT_COLUMNFAMILY = new Text("randomInt:");
  static final String GROUP_COLUMN_FAMILIES = "count: randomInt:";
  static final String BOOLEAN_TERM = "randomInt: > 100 AND count: <= 100 AND randomInt: !! 110|120|130|140|150";
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
    conf.setLong("hbase.hregion.max.filesize", 256 * 1024);
    dfsCluster = new MiniDFSCluster(conf, 1, true, (String[]) null);
    try {
      fs = dfsCluster.getFileSystem();
      dir = new Path("/hbase");
      fs.mkdirs(dir);
      // Start up HBase cluster
      hCluster = new MiniHBaseCluster(conf, 1, dfsCluster);
    } catch (Exception e) {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
        dfsCluster = null;
      }
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

    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }

    if (fs != null) {
      try {
        fs.close();
      } catch (IOException e) {
        LOG.info("During tear down got a " + e.getMessage());
      }
    }
  }

  public void testBooleanFilterMapReduce() {
    try {
      HTableDescriptor desc = new HTableDescriptor(INPUT_TABLE);
      String[] columns = GROUP_COLUMN_FAMILIES.split(" ");
      for (int i = 0; i < columns.length; i++) {
        desc.addFamily(new HColumnDescriptor(columns[i]));
      }
      HBaseAdmin admin = new HBaseAdmin(this.conf);
      admin.createTable(desc);

      // insert random data into the input table
      HTable table = new HTable(conf, new Text(INPUT_TABLE));
      Random oRandom = new Random();

      for (int j = 0; j < 200; j++) {
        int i = oRandom.nextInt(200) + 1;

        long lockid = table.startUpdate(new Text("rowKey" + j));
        table.put(lockid, COUNT_COLUMNFAMILY, Integer.toString(j).getBytes(
            HConstants.UTF8_ENCODING));
        table.put(lockid, RANDOMINT_COLUMNFAMILY, Integer.toString(i).getBytes(
            HConstants.UTF8_ENCODING));
        table.commit(lockid, System.currentTimeMillis());
      }

      long lockid = table.startUpdate(new Text("rowKey2001"));
      table.put(lockid, COUNT_COLUMNFAMILY, "12"
          .getBytes(HConstants.UTF8_ENCODING));
      table.put(lockid, RANDOMINT_COLUMNFAMILY, "120"
          .getBytes(HConstants.UTF8_ENCODING));
      table.commit(lockid, System.currentTimeMillis());

    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      HTableDescriptor output = new HTableDescriptor(OUTPUT_TABLE);
      String[] columns = GROUP_COLUMN_FAMILIES.split(" ");
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

      JobConf jobConf = new JobConf(conf, TestBooleanTermFilter.class);
      jobConf.setJobName("process boolean term filter mapreduce");
      jobConf.setNumMapTasks(2);
      jobConf.setNumReduceTasks(1);

      IdentityFilterMap.initJob(INPUT_TABLE, GROUP_COLUMN_FAMILIES,
          BOOLEAN_TERM, IdentityFilterMap.class, jobConf);

      IdentityTableReduce.initJob(OUTPUT_TABLE, IdentityTableReduce.class,
          jobConf);

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
   * Check the filtered value
   * 
   * @param conf
   * @param outputTable
   * @throws IOException
   */
  private void verify(HBaseConfiguration conf, String outputTable)
      throws IOException {
    HTable table = new HTable(conf, new Text(outputTable));
    Text[] columns = { COUNT_COLUMNFAMILY, RANDOMINT_COLUMNFAMILY };
    HScannerInterface scanner = table.obtainScanner(columns,
        HConstants.EMPTY_START_ROW);

    try {
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();

      int i = 0;
      while (scanner.next(key, results)) {
        for (Map.Entry<Text, byte[]> e : results.entrySet()) {
          if (e.getKey().equals(COUNT_COLUMNFAMILY)) {
            if (Integer.parseInt(new String(e.getValue())) > 100) {
              LOG.info("result_table.count: " + new String(e.getValue()));
            }
            assertTrue((Integer.parseInt(new String(e.getValue())) <= 100));
          } else {
            if (Integer.parseInt(new String(e.getValue())) <= 100
                || !checkNotInList(Integer.parseInt(new String(e.getValue())))) {
              LOG.info("result_table.randomInt: " + new String(e.getValue()));
            }
            assertTrue((Integer.parseInt(new String(e.getValue())) > 100 && checkNotInList(Integer
                .parseInt(new String(e.getValue())))));
          }
          i++;
        }
      }

      assertTrue(i > 0);
      if (i <= 0) {
        LOG.info("result_table.rowNumber: " + i);
      }

    } finally {
      scanner.close();
    }

  }

  /**
   * Check 'NOT IN' filter-list
   */
  private boolean checkNotInList(int parseInt) {
    return (parseInt != 110 && parseInt != 120 && parseInt != 130
        && parseInt != 140 && parseInt != 150) ? true : false;
  }
}
