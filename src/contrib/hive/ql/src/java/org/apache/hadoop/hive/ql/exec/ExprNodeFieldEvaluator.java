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
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;

import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ExprNodeFieldEvaluator extends ExprNodeEvaluator {

  protected exprNodeFieldDesc desc;
  transient ExprNodeEvaluator leftEvaluator;
  transient InspectableObject leftInspectableObject;
  transient StructObjectInspector cachedLeftObjectInspector;
  transient StructField field;
  transient ObjectInspector fieldObjectInspector;
  
  public ExprNodeFieldEvaluator(exprNodeFieldDesc desc) {
    this.desc = desc;
    leftEvaluator = ExprNodeEvaluatorFactory.get(desc.getDesc());
    field = null;
    leftInspectableObject = new InspectableObject();
  }

  public void evaluate(Object row, ObjectInspector rowInspector,
      InspectableObject result) throws HiveException {
    
    assert(result != null);
    // Get the result in leftInspectableObject
    leftEvaluator.evaluate(row, rowInspector, leftInspectableObject);

    if (field == null) {
      cachedLeftObjectInspector = (StructObjectInspector)leftInspectableObject.oi;
      field = cachedLeftObjectInspector.getStructFieldRef(desc.getFieldName());
      fieldObjectInspector = field.getFieldObjectInspector();
    } else {
      assert(cachedLeftObjectInspector == leftInspectableObject.oi);
    }
    result.oi = fieldObjectInspector;
    result.o = cachedLeftObjectInspector.getStructFieldData(leftInspectableObject.o, field); 
  }

  public ObjectInspector evaluateInspector(ObjectInspector rowInspector)
      throws HiveException {
    // If this is the first row, or the dynamic structure of the evaluatorInspectableObject 
    // is different from the previous row 
    leftInspectableObject.oi = leftEvaluator.evaluateInspector(rowInspector);
    if (field == null) {
      cachedLeftObjectInspector = (StructObjectInspector)leftInspectableObject.oi;
      field = cachedLeftObjectInspector.getStructFieldRef(desc.getFieldName());
      fieldObjectInspector = field.getFieldObjectInspector();
    } else {
      assert(cachedLeftObjectInspector == leftInspectableObject.oi);      
    }
    return fieldObjectInspector;
  }

}
