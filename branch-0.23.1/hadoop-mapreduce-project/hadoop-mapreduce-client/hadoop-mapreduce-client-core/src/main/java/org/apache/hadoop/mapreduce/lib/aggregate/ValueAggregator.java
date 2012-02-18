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

import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This interface defines the minimal protocol for value aggregators.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ValueAggregator<E> {

  /**
   * add a value to the aggregator
   * 
   * @param val the value to be added
   */
  public void addNextValue(Object val);

  /**
   * reset the aggregator
   *
   */
  public void reset();

  /**
   * @return the string representation of the agregator
   */
  public String getReport();

  /**
   * 
   * @return an array of values as the outputs of the combiner.
   */
  public ArrayList<E> getCombinerOutput();

}
