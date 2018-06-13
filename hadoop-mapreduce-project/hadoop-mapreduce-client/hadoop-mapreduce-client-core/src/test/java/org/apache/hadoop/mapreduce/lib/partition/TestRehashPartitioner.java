/**
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
package org.apache.hadoop.mapreduce.lib.partition;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import org.junit.*;

public class TestRehashPartitioner {

  /** number of partitions */
  private static final int PARTITIONS = 32;

  /** step in sequence */
  private static final int STEP = 3;

  /** end of test sequence */
  private static final int END = 100000;

  /** maximum error for considering too big/small bucket */
  private static final double MAX_ERROR = 0.20;

  /** maximum number of oddly sized buckets */
  private static final double MAX_BADBUCKETS = 0.10;

  /** test partitioner for patterns */
  @Test
  public void testPatterns() {
    int results[] = new int[PARTITIONS];
    RehashPartitioner <IntWritable, NullWritable> p = new RehashPartitioner < IntWritable, NullWritable> ();
    /* test sequence 4, 8, 12, ... 128 */
    for(int i = 0; i < END; i+= STEP) {
      results[p.getPartition(new IntWritable(i), null, PARTITIONS)]++;
    }
    int badbuckets = 0;
    Integer min = Collections.min(Arrays.asList(ArrayUtils.toObject(results)));
    Integer max = Collections.max(Arrays.asList(ArrayUtils.toObject(results)));
    Integer avg = (int) Math.round((max+min)/2.0);
    System.out.println("Dumping buckets distribution: min="+min+" avg="+avg+" max="+max);
    for (int i = 0; i < PARTITIONS; i++) {
      double var = (results[i]-avg)/(double)(avg);
      System.out.println("bucket "+i+" "+results[i]+" items, variance "+var);
      if (Math.abs(var) > MAX_ERROR)
        badbuckets++;
    }
    System.out.println(badbuckets + " of "+PARTITIONS+" are too small or large buckets");
    assertTrue("too many overflow buckets", badbuckets < PARTITIONS * MAX_BADBUCKETS);
  }
}
