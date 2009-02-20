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
import org.apache.hadoop.hive.ql.plan.exprNodeIndexDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public class ExprNodeIndexEvaluator extends ExprNodeEvaluator {

  protected exprNodeIndexDesc expr;
  transient ExprNodeEvaluator mainEvaluator;
  transient InspectableObject mainInspectableObject = new InspectableObject();
  transient ExprNodeEvaluator indexEvaluator;
  transient InspectableObject indexInspectableObject = new InspectableObject();
  
  public ExprNodeIndexEvaluator(exprNodeIndexDesc expr) {
    this.expr = expr;
    mainEvaluator = ExprNodeEvaluatorFactory.get(expr.getDesc());
    indexEvaluator = ExprNodeEvaluatorFactory.get(expr.getIndex());
  }

  public void evaluate(Object row, ObjectInspector rowInspector,
      InspectableObject result) throws HiveException {
    
    assert(result != null);
    mainEvaluator.evaluate(row, rowInspector, mainInspectableObject);
    indexEvaluator.evaluate(row, rowInspector, indexInspectableObject);

    if (mainInspectableObject.oi.getCategory() == Category.LIST) {
      int index = ((Number)indexInspectableObject.o).intValue();
    
      ListObjectInspector loi = (ListObjectInspector)mainInspectableObject.oi;
      result.oi = loi.getListElementObjectInspector();
      result.o = loi.getListElement(mainInspectableObject.o, index);
    }
    else if (mainInspectableObject.oi.getCategory() == Category.MAP) {
      MapObjectInspector moi = (MapObjectInspector)mainInspectableObject.oi;
      result.oi = moi.getMapValueObjectInspector();
      result.o = moi.getMapValueElement(mainInspectableObject.o, indexInspectableObject.o);
    }
    else {
      // Should never happen because we checked this in SemanticAnalyzer.getXpathOrFuncExprNodeDesc
      throw new RuntimeException("Hive 2 Internal error: cannot evaluate index expression on "
          + mainInspectableObject.oi.getTypeName());
    }
  }

  public ObjectInspector evaluateInspector(ObjectInspector rowInspector)
      throws HiveException {
    ObjectInspector mainInspector = mainEvaluator.evaluateInspector(rowInspector);
    if (mainInspector.getCategory() == Category.LIST) {
      return ((ListObjectInspector)mainInspector).getListElementObjectInspector();
    } else if (mainInspector.getCategory() == Category.MAP) {
      return ((MapObjectInspector)mainInspector).getMapValueObjectInspector();
    } else {
      // Should never happen because we checked this in SemanticAnalyzer.getXpathOrFuncExprNodeDesc
      throw new RuntimeException("Hive 2 Internal error: cannot evaluate index expression on "
          + mainInspector.getTypeName());
    }
  }

}
