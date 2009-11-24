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

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test of simple partitioner.
 */
public class TestSimpleTotalOrderPartitioner extends HBaseTestCase {
  public void testSplit() throws Exception {
    String start = "a";
    String end = "{";
    SimpleTotalOrderPartitioner<byte []> p =
      new SimpleTotalOrderPartitioner<byte []>();
    this.conf.set(SimpleTotalOrderPartitioner.START, start);
    this.conf.set(SimpleTotalOrderPartitioner.END, end);
    p.setConf(this.conf);
    ImmutableBytesWritable c = new ImmutableBytesWritable(Bytes.toBytes("c"));
    // If one reduce, partition should be 0.
    int partition = p.getPartition(c, HConstants.EMPTY_BYTE_ARRAY, 1);
    assertEquals(0, partition);
    // If two reduces, partition should be 0.
    partition = p.getPartition(c, HConstants.EMPTY_BYTE_ARRAY, 2);
    assertEquals(0, partition);
    // Divide in 3.
    partition = p.getPartition(c, HConstants.EMPTY_BYTE_ARRAY, 3);
    assertEquals(0, partition);
    ImmutableBytesWritable q = new ImmutableBytesWritable(Bytes.toBytes("q"));
    partition = p.getPartition(q, HConstants.EMPTY_BYTE_ARRAY, 2);
    assertEquals(1, partition);
    partition = p.getPartition(q, HConstants.EMPTY_BYTE_ARRAY, 3);
    assertEquals(2, partition);
    // What about end and start keys.
    ImmutableBytesWritable startBytes =
      new ImmutableBytesWritable(Bytes.toBytes(start));
    partition = p.getPartition(startBytes, HConstants.EMPTY_BYTE_ARRAY, 2);
    assertEquals(0, partition);
    partition = p.getPartition(startBytes, HConstants.EMPTY_BYTE_ARRAY, 3);
    assertEquals(0, partition);
    ImmutableBytesWritable endBytes =
      new ImmutableBytesWritable(Bytes.toBytes("z"));
    partition = p.getPartition(endBytes, HConstants.EMPTY_BYTE_ARRAY, 2);
    assertEquals(1, partition);
    partition = p.getPartition(endBytes, HConstants.EMPTY_BYTE_ARRAY, 3);
    assertEquals(2, partition);
  }
}
