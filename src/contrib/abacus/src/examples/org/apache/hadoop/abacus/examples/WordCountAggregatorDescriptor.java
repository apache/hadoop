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

package org.apache.hadoop.abacus.examples;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.abacus.ValueAggregatorBaseDescriptor;
import org.apache.hadoop.mapred.JobConf;

/**
 * This class implements a user defined aggregator descriptor that is used
 * for counting the words in the input data
 *
 */
public class WordCountAggregatorDescriptor extends
    ValueAggregatorBaseDescriptor {

  /**
   * Parse the given value, generate an aggregation-id/value pair per word.
   * The ID is of type LONG_VALUE_SUM, with WORD as the real id. The value is 1.
   * 
   * @return a list of the generated pairs.
   */
  public ArrayList<Entry> generateKeyValPairs(Object key, Object val) {
    String words[] = val.toString().split(" |\t");
    ArrayList<Entry> retv = new ArrayList<Entry>();
    for (int i = 0; i < words.length; i++) {
      Entry en = generateEntry(LONG_VALUE_SUM, words[i], ONE);
      retv.add(en);
    }
    return retv;
  }

  /** 
   * Do nothing.
   */
  public void configure(JobConf job) {

  }

}
