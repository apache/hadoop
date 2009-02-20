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

package org.apache.hadoop.hive.ql.exec;


import org.apache.hadoop.hive.ql.metadata.HiveException;
//import org.apache.hadoop.hive.serde.ReflectionSerDe;

/**
 * Base class for all User-defined Aggregation Function (UDAF) classes.
 *
 * UDAF classes are REQUIRED to inherit from this class.
 *
 * Required for a UDAF class:
 * 1. Implement the init() method, which reset the status of the aggregation function.
 * 2. Implement a single method called "aggregate" that returns boolean.  The method should
 *    always return "true" on valid inputs, or the framework will throw an Exception.
 *    Following are some examples:
 *    public boolean aggregate(double a);
 *    public boolean aggregate(int b);
 *    public boolean aggregate(double c, double d);
 * 3. Implement a single method called "evaluate" that returns the FINAL aggregation result.
 *    "evaluate" should never return "null" or an Exception will be thrown.
 *    Following are some examples.
 *    public int evaluate();
 *    public long evaluate();
 *    public double evaluate();
 *    public Double evaluate();
 *    public String evaluate();
 *
 * Optional for a UDAF class (by implementing these 2 methods, the user declares that the
 * UDAF support partial aggregations):
 * 1. Implement a single method called "evaluatePartial" that returns the PARTIAL aggregation
 *    result. "evaluatePartial" should never return "null" or an Exception will be thrown.
 * 2. Implement a single method called "aggregatePartial" that takes a PARTIAL aggregation
 *    result and returns a boolean.  The method should always return "true" on valid inputs,
 *    or the framework will throw an Exception.
 *
 *    Following are some examples:
 *    public int evaluatePartial();
 *    public boolean aggregatePartial(int partial);
 *
 *    public String evaluatePartial();
 *    public boolean aggregatePartial(String partial);
 *
 */
public abstract class UDAF {

  public UDAF() { }

  /** Initialize the aggregation object.
   *  The class should reset the status of the aggregation if aggregate() was called before.
   */
  public abstract void init();
}
