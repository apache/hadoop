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
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * This class support multi-level fields like "a.b.c" for historical reasons.
 */
public class ExprNodeColumnEvaluator extends ExprNodeEvaluator {

  protected exprNodeColumnDesc expr;
  transient StructObjectInspector cachedRowInspector;
  transient String[] fieldNames;
  transient StructField[] fields;
  transient ObjectInspector[] fieldsObjectInspector;
  
  public ExprNodeColumnEvaluator(exprNodeColumnDesc expr) {
    this.expr = expr;
  }

  public void evaluate(Object row, ObjectInspector rowInspector,
      InspectableObject result) throws HiveException {
    
    assert(result != null);
    // If this is the first row, or the dynamic structure of this row 
    // is different from the previous row 
    if (fields == null || cachedRowInspector != rowInspector) {
      evaluateInspector(rowInspector);
    }
    result.o = cachedRowInspector.getStructFieldData(row, fields[0]);
    for(int i=1; i<fields.length; i++) {
      result.o = ((StructObjectInspector)fieldsObjectInspector[i-1]).getStructFieldData(
          result.o, fields[i]);
    }
    result.oi = fieldsObjectInspector[fieldsObjectInspector.length - 1];
  }

  public ObjectInspector evaluateInspector(ObjectInspector rowInspector)
      throws HiveException {
    
    if (fields == null || cachedRowInspector != rowInspector) {
      cachedRowInspector = (StructObjectInspector)rowInspector;
      fieldNames = expr.getColumn().split("\\.", -1);
      fields = new StructField[fieldNames.length];
      fieldsObjectInspector = new ObjectInspector[fieldNames.length];
      fields[0] = cachedRowInspector.getStructFieldRef(fieldNames[0]);
      fieldsObjectInspector[0] = fields[0].getFieldObjectInspector();
      for (int i=1; i<fieldNames.length; i++) {
        fields[i] = ((StructObjectInspector)fieldsObjectInspector[i-1]).getStructFieldRef(fieldNames[i]);
        fieldsObjectInspector[i] = fields[i].getFieldObjectInspector();
      }
    }
    return fieldsObjectInspector[fieldsObjectInspector.length - 1];
  }
}
