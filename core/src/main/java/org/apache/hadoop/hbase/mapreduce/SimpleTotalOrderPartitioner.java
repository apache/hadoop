/**
 * Copyright 2009 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A partitioner that takes start and end keys and uses bigdecimal to figure
 * which reduce a key belongs to.  Pass the start and end
 * keys in the Configuration using <code>hbase.simpletotalorder.start</code>
 * and <code>hbase.simpletotalorder.end</code>.  The end key needs to be
 * exclusive; i.e. one larger than the biggest key in your key space.
 * You may be surprised at how this class partitions the space; it may not
 * align with preconceptions; e.g. a start key of zero and an end key of 100
 * divided in ten will not make regions whose range is 0-10, 10-20, and so on.
 * Make your own partitioner if you need the region spacing to come out a
 * particular way.
 * @param <VALUE>
 * @see #START
 * @see #END
 */
public class SimpleTotalOrderPartitioner<VALUE> extends Partitioner<ImmutableBytesWritable, VALUE>
implements Configurable {
  private final Log LOG = LogFactory.getLog(this.getClass());
  public static final String START = "hbase.simpletotalorder.start";
  public static final String END = "hbase.simpletotalorder.end";
  private Configuration c;
  private byte [] startkey;
  private byte [] endkey;
  private byte [][] splits;
  private int lastReduces = -1;
  
  @Override
  public int getPartition(final ImmutableBytesWritable key, final VALUE value,
      final int reduces) {
    if (reduces == 1) return 0;
    if (this.lastReduces != reduces) {
      this.splits = Bytes.split(this.startkey, this.endkey, reduces - 1);
      for (int i = 0; i < splits.length; i++) {
        LOG.info(Bytes.toString(splits[i]));
      }
    }
    int pos = Bytes.binarySearch(this.splits, key.get(), key.getOffset(),
      key.getLength(), Bytes.BYTES_RAWCOMPARATOR);
    // Below code is from hfile index search.
    if (pos < 0) {
      pos++;
      pos *= -1;
      if (pos == 0) {
        // falls before the beginning of the file.
        throw new RuntimeException("Key outside start/stop range: " +
          key.toString());
      }
      pos--;
    }
    return pos;
  }

  @Override
  public Configuration getConf() {
    return this.c;
  }

  @Override
  public void setConf(Configuration conf) {
    this.c = conf;
    String startStr = this.c.get(START);
    String endStr = this.c.get(END);
    LOG.info("startkey=" + startStr + ", endkey=" + endStr);
    this.startkey = Bytes.toBytes(startStr);
    this.endkey = Bytes.toBytes(endStr);
  }
}