/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.dfs.MiniDFSCluster;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.hbase.io.KeyedData;
import org.apache.hadoop.hbase.io.KeyedDataArrayWritable;

import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableOutputCollector;
import org.apache.hadoop.hbase.mapred.IdentityTableReduce;

/**
 * Test Map/Reduce job over HBase tables
 */
public class TestTableMapReduce extends HBaseTestCase {
  static final String TABLE_NAME = "test";
  static final String INPUT_COLUMN = "contents:";
  static final Text TEXT_INPUT_COLUMN = new Text(INPUT_COLUMN);
  static final String OUTPUT_COLUMN = "text:";
  static final Text TEXT_OUTPUT_COLUMN = new Text(OUTPUT_COLUMN);
  
  private Random rand;
  private HTableDescriptor desc;

  private MiniDFSCluster dfsCluster = null;
  private FileSystem fs;
  private Path dir;
  private MiniHBaseCluster hCluster = null;
  
  private byte[][] values = {
      "0123".getBytes(),
      "abcd".getBytes(),
      "wxyz".getBytes(),
      "6789".getBytes()
  };

  /**
   * {@inheritDoc}
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    rand = new Random();
    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(INPUT_COLUMN));
    desc.addFamily(new HColumnDescriptor(OUTPUT_COLUMN));
    
    dfsCluster = new MiniDFSCluster(conf, 1, true, (String[])null);
    try {
      fs = dfsCluster.getFileSystem();
      dir = new Path("/hbase");
      fs.mkdirs(dir);

      // create the root and meta regions and insert the data region into the meta

      HRegion root = createNewHRegion(fs, dir, conf, HGlobals.rootTableDesc, 0L, null, null);
      HRegion meta = createNewHRegion(fs, dir, conf, HGlobals.metaTableDesc, 1L, null, null);
      HRegion.addRegionToMETA(root, meta);

      HRegion region = createNewHRegion(fs, dir, conf, desc, rand.nextLong(), null, null);
      HRegion.addRegionToMETA(meta, region);

      // insert some data into the test table

      for(int i = 0; i < values.length; i++) {
        long lockid = region.startUpdate(new Text("row_"
            + String.format("%1$05d", i)));

        region.put(lockid, TEXT_INPUT_COLUMN, values[i]);
        region.commit(lockid, System.currentTimeMillis());
      }

      region.close();
      region.getLog().closeAndDelete();
      meta.close();
      meta.getLog().closeAndDelete();
      root.close();
      root.getLog().closeAndDelete();

      // Start up HBase cluster

      hCluster = new MiniHBaseCluster(conf, 1, dfsCluster);
      
    } catch (Exception e) {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    
    if(hCluster != null) {
      hCluster.shutdown();
    }
    
  }

  /**
   * Pass the given key and processed record reduce
   */
  public static class ProcessContentsMapper extends TableMap {

    /** constructor */
    public ProcessContentsMapper() {
      super();
    }

    /**
     * Pass the key, and reversed value to reduce
     *
     * @see org.apache.hadoop.hbase.mapred.TableMap#map(org.apache.hadoop.hbase.HStoreKey, org.apache.hadoop.hbase.io.KeyedDataArrayWritable, org.apache.hadoop.hbase.mapred.TableOutputCollector, org.apache.hadoop.mapred.Reporter)
     */
    @Override
    public void map(HStoreKey key, KeyedDataArrayWritable value,
        TableOutputCollector output,
        @SuppressWarnings("unused") Reporter reporter) throws IOException {
      
      Text tKey = key.getRow();
      KeyedData[] columns = value.get();
      
      if(columns.length != 1) {
        throw new IOException("There should only be one input column");
      }
      
      if(!columns[0].getKey().getColumn().equals(TEXT_INPUT_COLUMN)) {
        throw new IOException("Wrong input column. Expected: " + INPUT_COLUMN
            + " but got: " + columns[0].getKey().getColumn());
      }

      // Get the input column key and change it to the output column key
      
      HStoreKey column = columns[0].getKey();
      column.setColumn(TEXT_OUTPUT_COLUMN);
      
      // Get the original value and reverse it
      
      String originalValue = new String(columns[0].getData());
      StringBuilder newValue = new StringBuilder();
      for(int i = originalValue.length() - 1; i >= 0; i--) {
        newValue.append(originalValue.charAt(i));
      }
      
      // Now set the value to be collected
      
      columns[0] = new KeyedData(column, newValue.toString().getBytes());
      value.set(columns);
      
      output.collect(tKey, value);
    }
  }

  /**
   * Test HBase map/reduce
   * @throws IOException
   */
  @SuppressWarnings("static-access")
  public void testTableMapReduce() throws IOException {
    System.out.println("Print table contents before map/reduce");
    scanTable(conf);
    
    @SuppressWarnings("deprecation")
    MiniMRCluster mrCluster = new MiniMRCluster(2, fs.getName(), 1);

    try {
      JobConf jobConf = new JobConf(conf, TestTableMapReduce.class);
      jobConf.setJobName("process column contents");
      jobConf.setNumMapTasks(1);
      jobConf.setNumReduceTasks(1);

      ProcessContentsMapper.initJob(TABLE_NAME, INPUT_COLUMN, 
          ProcessContentsMapper.class, jobConf);

      IdentityTableReduce.initJob(TABLE_NAME, IdentityTableReduce.class, jobConf);

      JobClient.runJob(jobConf);
      
    } finally {
      mrCluster.shutdown();
    }
    
    System.out.println("Print table contents after map/reduce");
    scanTable(conf);
  }
  
  private void scanTable(Configuration conf) throws IOException {
    HClient client = new HClient(conf);
    client.openTable(new Text(TABLE_NAME));
    
    Text[] columns = {
        TEXT_INPUT_COLUMN,
        TEXT_OUTPUT_COLUMN
    };
    HScannerInterface scanner =
      client.obtainScanner(columns, HClient.EMPTY_START_ROW);
    
    try {
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      
      while(scanner.next(key, results)) {
        System.out.print("row: " + key.getRow());
        
        for(Map.Entry<Text, byte[]> e: results.entrySet()) {
          System.out.print(" column: " + e.getKey() + " value: "
              + new String(e.getValue()));
        }
        System.out.println();
      }
      
    } finally {
      scanner.close();
    }
  }
}
