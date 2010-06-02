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
import org.apache.hadoop.hbase.util.Base64;
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
  private final static Log LOG = LogFactory.getLog(SimpleTotalOrderPartitioner.class);

  @Deprecated
  public static final String START = "hbase.simpletotalorder.start";
  @Deprecated
  public static final String END = "hbase.simpletotalorder.end";
  
  static final String START_BASE64 = "hbase.simpletotalorder.start.base64";
  static final String END_BASE64 = "hbase.simpletotalorder.end.base64";
  
  private Configuration c;
  private byte [] startkey;
  private byte [] endkey;
  private byte [][] splits;
  private int lastReduces = -1;

  public static void setStartKey(Configuration conf, byte[] startKey) {
    conf.set(START_BASE64, Base64.encodeBytes(startKey));
  }
  
  public static void setEndKey(Configuration conf, byte[] endKey) {
    conf.set(END_BASE64, Base64.encodeBytes(endKey));
  }
  
  @SuppressWarnings("deprecation")
  static byte[] getStartKey(Configuration conf) {
    return getKeyFromConf(conf, START_BASE64, START);
  }
  
  @SuppressWarnings("deprecation")
  static byte[] getEndKey(Configuration conf) {
    return getKeyFromConf(conf, END_BASE64, END);
  }
  
  private static byte[] getKeyFromConf(Configuration conf,
      String base64Key, String deprecatedKey) {
    String encoded = conf.get(base64Key);
    if (encoded != null) {
      return Base64.decode(encoded);
    }
    String oldStyleVal = conf.get(deprecatedKey);
    if (oldStyleVal == null) {
      return null;
    }
    LOG.warn("Using deprecated configuration " + deprecatedKey +
        " - please use static accessor methods instead.");
    return Bytes.toBytes(oldStyleVal);
  }
  
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
    this.startkey = getStartKey(conf);
    this.endkey = getEndKey(conf);
    if (startkey == null || endkey == null) {
      throw new RuntimeException(this.getClass() + " not configured");
    }
    LOG.info("startkey=" + Bytes.toStringBinary(startkey) +
        ", endkey=" + Bytes.toStringBinary(endkey));
  }
}