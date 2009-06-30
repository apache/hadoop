/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This is used to partition the output keys into groups of keys.
 * Keys are grouped according to the regions that currently exist
 * so that each reducer fills a single region so load is distributed.
 * 
 * @param <KEY>  The type of the key.
 * @param <VALUE>  The type of the value.
 */
public class HRegionPartitioner<KEY, VALUE> 
extends Partitioner<ImmutableBytesWritable, VALUE>
implements Configurable {
  
  private final Log LOG = LogFactory.getLog(TableInputFormat.class);
  private Configuration conf = null;
  private HTable table;
  private byte[][] startKeys; 
  
  /**
   * Gets the partition number for a given key (hence record) given the total 
   * number of partitions i.e. number of reduce-tasks for the job.
   *   
   * <p>Typically a hash function on a all or a subset of the key.</p>
   *
   * @param key  The key to be partitioned.
   * @param value  The entry value.
   * @param numPartitions  The total number of partitions.
   * @return The partition number for the <code>key</code>.
   * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(
   *   java.lang.Object, java.lang.Object, int)
   */
  @Override
  public int getPartition(ImmutableBytesWritable key,
      VALUE value, int numPartitions) {
    byte[] region = null;
    // Only one region return 0
    if (this.startKeys.length == 1){
      return 0;
    }
    try {
      // Not sure if this is cached after a split so we could have problems
      // here if a region splits while mapping
      region = table.getRegionLocation(key.get()).getRegionInfo().getStartKey();
    } catch (IOException e) {
      LOG.error(e);
    }
    for (int i = 0; i < this.startKeys.length; i++){
      if (Bytes.compareTo(region, this.startKeys[i]) == 0 ){
        if (i >= numPartitions-1){
          // cover if we have less reduces then regions.
          return (Integer.toString(i).hashCode() 
              & Integer.MAX_VALUE) % numPartitions;
        }
        return i;
      }
    }
    // if above fails to find start key that match we need to return something
    return 0;
  }

  /**
   * Returns the current configuration.
   *  
   * @return The current configuration.
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Sets the configuration. This is used to determine the start keys for the
   * given table.
   * 
   * @param configuration  The configuration to set.
   * @see org.apache.hadoop.conf.Configurable#setConf(
   *   org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    try {
      this.table = new HTable(new HBaseConfiguration(conf), 
        configuration.get(TableOutputFormat.OUTPUT_TABLE));
    } catch (IOException e) {
      LOG.error(e);
    }
    try {
      this.startKeys = this.table.getStartKeys();
    } catch (IOException e) {
      LOG.error(e);
    }
  }
}
