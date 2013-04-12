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

package org.apache.hadoop.mapred.lib.aggregate;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;

/**
 * This class implements a wrapper for a user defined value aggregator 
 * descriptor.
 * It serves two functions: One is to create an object of 
 * ValueAggregatorDescriptor from the name of a user defined class that may be 
 * dynamically loaded. The other is to delegate invocations of 
 * generateKeyValPairs function to the created object.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UserDefinedValueAggregatorDescriptor extends org.apache.hadoop.
    mapreduce.lib.aggregate.UserDefinedValueAggregatorDescriptor
    implements ValueAggregatorDescriptor {

  /**
   * Create an instance of the given class
   * @param className the name of the class
   * @return a dynamically created instance of the given class 
   */
  public static Object createInstance(String className) {
    return org.apache.hadoop.mapreduce.lib.aggregate.
      UserDefinedValueAggregatorDescriptor.createInstance(className);
  }

  /**
   * 
   * @param className the class name of the user defined descriptor class
   * @param job a configure object used for decriptor configuration
   */
  public UserDefinedValueAggregatorDescriptor(String className, JobConf job) {
    super(className, job);
    ((ValueAggregatorDescriptor)theAggregatorDescriptor).configure(job);
  }

  /**
   *  Do nothing.
   */
  public void configure(JobConf job) {

  }

}
