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

import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Select operator implementation
 **/
public class SelectOperator extends Operator <selectDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ExprNodeEvaluator[] eval;

  transient ArrayList<Object> output;
  transient ArrayList<ObjectInspector> outputFieldObjectInspectors;
  transient ObjectInspector outputObjectInspector;
  transient InspectableObject tempInspectableObject;
  
  boolean firstRow;
  
  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    try {
      ArrayList<exprNodeDesc> colList = conf.getColList();
      eval = new ExprNodeEvaluator[colList.size()];
      for(int i=0; i<colList.size(); i++) {
        assert(colList.get(i) != null);
        eval[i] = ExprNodeEvaluatorFactory.get(colList.get(i));
      }
      output = new ArrayList<Object>(eval.length);
      outputFieldObjectInspectors = new ArrayList<ObjectInspector>(eval.length);
      for(int j=0; j<eval.length; j++) {
        output.add(null);
        outputFieldObjectInspectors.add(null);
      }
      tempInspectableObject = new InspectableObject();      
      firstRow = true;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void process(Object row, ObjectInspector rowInspector)
      throws HiveException {
    for(int i=0; i<eval.length; i++) {
      eval[i].evaluate(row, rowInspector, tempInspectableObject);
      output.set(i, tempInspectableObject.o);
      if (firstRow) {
        outputFieldObjectInspectors.set(i, tempInspectableObject.oi);
      }
    }
    if (firstRow) {
      firstRow = false;
      ArrayList<String> fieldNames = new ArrayList<String>(eval.length);
      for(int i=0; i<eval.length; i++) {
        fieldNames.add(Integer.valueOf(i).toString());
      }
      outputObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        fieldNames, outputFieldObjectInspectors);
    }
    forward(output, outputObjectInspector);
  }
}
