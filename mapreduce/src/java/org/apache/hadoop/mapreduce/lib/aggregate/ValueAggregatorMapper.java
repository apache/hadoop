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

package org.apache.hadoop.mapreduce.lib.aggregate;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class implements the generic mapper of Aggregate.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorMapper<K1 extends WritableComparable<?>,
                                   V1 extends Writable>
  extends Mapper<K1, V1, Text, Text> {

  public void setup(Context context) 
      throws IOException, InterruptedException {
    ValueAggregatorJobBase.setup(context.getConfiguration());
  }
  
  /**
   *  the map function. It iterates through the value aggregator descriptor 
   *  list to generate aggregation id/value pairs and emit them.
   */
  public void map(K1 key, V1 value,
      Context context) throws IOException, InterruptedException  {

    Iterator<?> iter = 
      ValueAggregatorJobBase.aggregatorDescriptorList.iterator();
    while (iter.hasNext()) {
      ValueAggregatorDescriptor ad = (ValueAggregatorDescriptor) iter.next();
      Iterator<Entry<Text, Text>> ens =
        ad.generateKeyValPairs(key, value).iterator();
      while (ens.hasNext()) {
        Entry<Text, Text> en = ens.next();
        context.write(en.getKey(), en.getValue());
      }
    }
  }
}
