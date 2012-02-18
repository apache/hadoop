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
package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;

import junit.framework.TestCase;

public class TestKeyFieldBasedPartitioner extends TestCase {

  /**
   * Test is key-field-based partitioned works with empty key.
   */
  public void testEmptyKey() throws Exception {
    KeyFieldBasedPartitioner<Text, Text> kfbp = 
      new KeyFieldBasedPartitioner<Text, Text>();
    JobConf conf = new JobConf();
    conf.setInt("num.key.fields.for.partition", 10);
    kfbp.configure(conf);
    assertEquals("Empty key should map to 0th partition", 
                 0, kfbp.getPartition(new Text(), new Text(), 10));
  }
}