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
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public abstract class ExprNodeEvaluator {

  /**
   * Evaluate the expression given the row and rowInspector. 
   * @param result   result.o and result.oi will be set inside the method.
   */
  public abstract void evaluate(Object row, ObjectInspector rowInspector, InspectableObject result) throws HiveException;

  /**
   * Metadata evaluation. Return the inspector for the expression, given the rowInspector.
   * This method must return the same value as result.oi in evaluate(...) call with the same rowInspector.   
   */
  public abstract ObjectInspector evaluateInspector(ObjectInspector rowInspector) throws HiveException;

}
