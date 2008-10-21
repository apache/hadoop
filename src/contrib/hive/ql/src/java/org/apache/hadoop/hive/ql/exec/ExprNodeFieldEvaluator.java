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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;

import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ExprNodeFieldEvaluator extends ExprNodeEvaluator {

  protected exprNodeFieldDesc desc;
  transient ExprNodeEvaluator leftEvaluator;
  transient InspectableObject leftInspectableObject;
  transient StructObjectInspector structObjectInspector;
  transient StructField field;
  transient ObjectInspector structFieldObjectInspector;
  transient ObjectInspector resultObjectInspector;
  
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
      evaluateInspector(rowInspector);
    }
    result.oi = resultObjectInspector;
    if (desc.getIsList()) {
      List<?> list = ((ListObjectInspector)leftInspectableObject.oi).getList(leftInspectableObject.o);
      List<Object> r = new ArrayList<Object>(list.size());
      for(int i=0; i<list.size(); i++) {
        r.add(structObjectInspector.getStructFieldData(list.get(i), field));
      }
      result.o = r;
    } else {
      result.o = structObjectInspector.getStructFieldData(leftInspectableObject.o, field);
    }
  }

  public ObjectInspector evaluateInspector(ObjectInspector rowInspector)
      throws HiveException {
    // If this is the first row, or the dynamic structure of the evaluatorInspectableObject 
    // is different from the previous row 
    leftInspectableObject.oi = leftEvaluator.evaluateInspector(rowInspector);
    if (field == null) {
      if (desc.getIsList()) {
        structObjectInspector = (StructObjectInspector)((ListObjectInspector)leftInspectableObject.oi).getListElementObjectInspector();
      } else {
        structObjectInspector = (StructObjectInspector)leftInspectableObject.oi;
      }
      field = structObjectInspector.getStructFieldRef(desc.getFieldName());
      structFieldObjectInspector = field.getFieldObjectInspector();
    }
    if (desc.getIsList()) {
      resultObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(structFieldObjectInspector);
    } else {
      resultObjectInspector = structFieldObjectInspector;
    }
    return resultObjectInspector;
  }

}
