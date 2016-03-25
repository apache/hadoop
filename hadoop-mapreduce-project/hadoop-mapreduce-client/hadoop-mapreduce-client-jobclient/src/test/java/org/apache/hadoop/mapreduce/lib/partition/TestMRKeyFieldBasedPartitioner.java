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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMRKeyFieldBasedPartitioner {

  /**
   * Test is key-field-based partitioned works with empty key.
   */
  @Test
  public void testEmptyKey() throws Exception {
    int numReducers = 10;
    KeyFieldBasedPartitioner<Text, Text> kfbp = 
      new KeyFieldBasedPartitioner<Text, Text>();
    Configuration conf = new Configuration();
    conf.setInt("num.key.fields.for.partition", 10);
    kfbp.setConf(conf);
    assertEquals("Empty key should map to 0th partition", 
                 0, kfbp.getPartition(new Text(), new Text(), numReducers));
    
    // check if the hashcode is correct when no keyspec is specified
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    kfbp.setConf(conf);
    String input = "abc\tdef\txyz";
    int hashCode = input.hashCode();
    int expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // check if the hashcode is correct with specified keyspec
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2");
    kfbp.setConf(conf);
    String expectedOutput = "def";
    byte[] eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with invalid end index in keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,5");
    kfbp.setConf(conf);
    expectedOutput = "def\txyz";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with 0 end index in keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2");
    kfbp.setConf(conf);
    expectedOutput = "def\txyz";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with invalid keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k10");
    kfbp.setConf(conf);
    assertEquals("Partitioner doesnt work as expected", 0, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with multiple keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2 -k4,4");
    kfbp.setConf(conf);
    input = "abc\tdef\tpqr\txyz";
    expectedOutput = "def";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedOutput = "xyz";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, hashCode);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with invalid start index in keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2 -k30,21 -k4,4 -k5");
    kfbp.setConf(conf);
    expectedOutput = "def";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedOutput = "xyz";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, hashCode);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
  }
}
