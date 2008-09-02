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
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.util.ReflectionUtils;

public class ExprNodeFuncEvaluator extends ExprNodeEvaluator {

  private static final Log LOG = LogFactory.getLog(ExprNodeFuncEvaluator.class.getName());
  
  protected exprNodeFuncDesc expr;
  transient ArrayList<ExprNodeEvaluator> evaluators;
  transient Object[] children;
  transient UDF udf;
  
  public ExprNodeFuncEvaluator(exprNodeFuncDesc expr) {
    this.expr = expr;
    assert(expr != null);
    Class<?> c = expr.getUDFClass();
    LOG.info(c.toString());
    udf = (UDF)ReflectionUtils.newInstance(expr.getUDFClass(), null);
    evaluators = new ArrayList<ExprNodeEvaluator>();
    for(int i=0; i<expr.getChildren().size(); i++) {
      evaluators.add(ExprNodeEvaluatorFactory.get(expr.getChildren().get(i)));
    }
    children = new Object[expr.getChildren().size()];  
  }

  public Object evaluateToObject(HiveObject row)  throws HiveException {
    // Evaluate all children first
    for(int i=0; i<evaluators.size(); i++) {
      Object o = evaluators.get(i).evaluateToObject(row);
      children[i] = o;
    }
    try {
      return expr.getUDFMethod().invoke(udf, children);
    } catch (Exception e) {
      throw new HiveException("Unable to execute UDF function " + udf.getClass() + " " 
          + expr.getUDFMethod() + " on inputs " + "(" + children.length + ") " + Arrays.asList(children) + ": " + e.getMessage(), e);
    }
  }

  public HiveObject evaluate(HiveObject row) throws HiveException {
    Object obj = evaluateToObject(row);
    if (obj == null)
      return new NullHiveObject();
    return new PrimitiveHiveObject(obj);
  }
}
