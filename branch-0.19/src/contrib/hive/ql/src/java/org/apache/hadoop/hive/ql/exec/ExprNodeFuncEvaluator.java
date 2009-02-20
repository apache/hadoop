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

import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.util.ReflectionUtils;

public class ExprNodeFuncEvaluator extends ExprNodeEvaluator {

  private static final Log LOG = LogFactory.getLog(ExprNodeFuncEvaluator.class.getName());
  
  protected exprNodeFuncDesc expr;
  transient ExprNodeEvaluator[] paramEvaluators;
  transient InspectableObject[] paramInspectableObjects;
  transient Object[] paramValues;
  transient UDF udf;
  transient Method udfMethod;
  transient ObjectInspector outputObjectInspector;
  
  public ExprNodeFuncEvaluator(exprNodeFuncDesc expr) {
    this.expr = expr;
    assert(expr != null);
    Class<?> c = expr.getUDFClass();
    udfMethod = expr.getUDFMethod();
    LOG.debug(c.toString());
    LOG.debug(udfMethod.toString());
    udf = (UDF)ReflectionUtils.newInstance(expr.getUDFClass(), null);
    int paramNumber = expr.getChildren().size();
    paramEvaluators = new ExprNodeEvaluator[paramNumber];
    paramInspectableObjects  = new InspectableObject[paramNumber];
    for(int i=0; i<paramNumber; i++) {
      paramEvaluators[i] = ExprNodeEvaluatorFactory.get(expr.getChildren().get(i));
      paramInspectableObjects[i] = new InspectableObject();
    }
    paramValues = new Object[expr.getChildren().size()];
    outputObjectInspector = ObjectInspectorFactory.getStandardPrimitiveObjectInspector(
        udfMethod.getReturnType());
  }

  public void evaluate(Object row, ObjectInspector rowInspector,
      InspectableObject result) throws HiveException {
    if (result == null) {
      throw new HiveException("result cannot be null.");
    }
    // Evaluate all children first
    for(int i=0; i<paramEvaluators.length; i++) {
      paramEvaluators[i].evaluate(row, rowInspector, paramInspectableObjects[i]);
      paramValues[i] = paramInspectableObjects[i].o;
    }
    try {
      result.o = udfMethod.invoke(udf, paramValues);
      result.oi = outputObjectInspector;
    } catch (Exception e) {
      if (e instanceof HiveException) {
        throw (HiveException)e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
        throw new HiveException("Unable to execute UDF function " + udf.getClass() + " " 
          + udfMethod + " on inputs " + "(" + paramValues.length + ") " + Arrays.asList(paramValues) + ": " + e.getMessage(), e);
      }
    }
  }

  public ObjectInspector evaluateInspector(ObjectInspector rowInspector)
      throws HiveException {
    return outputObjectInspector;
  }
}
