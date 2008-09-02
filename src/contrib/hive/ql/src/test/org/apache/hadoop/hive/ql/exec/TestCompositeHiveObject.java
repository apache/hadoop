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

import junit.framework.TestCase;
import java.io.*;
import java.util.*;

import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.serde.thrift.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.TypeInfo;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes;

public class TestCompositeHiveObject extends TestCase {

  // this is our row to test expressions on
  protected HiveObject [] r;
  protected CompositeHiveObject cr;

  protected void setUp() throws HiveException {
    r = new HiveObject [5];
    for(int i=0; i<5; i++) {
      ArrayList<String> data = new ArrayList<String> ();
      data.add(""+i);
      data.add(""+(i+1));
      data.add(""+(i+2));
      ColumnSet cs = new ColumnSet(data);
      try {
        r[i] = new TableHiveObject(cs, new columnsetSerDe());
      } catch (Exception e) {
          e.printStackTrace();
        throw new RuntimeException (e);
      }
    }
    cr = new CompositeHiveObject(5);
    for(int i=0; i<5; i++) {
      cr.addHiveObject(r[i]);
    }

  }

  public void testCompositeHiveObjectExpression() throws Exception {
    try {
      // get a evaluator for a simple field expression
      exprNodeColumnDesc exprDesc = new exprNodeColumnDesc(String.class, "0.col[1]");
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(exprDesc);

      // evaluate on row
      Object ret = eval.evaluateToObject(cr);
      assertEquals(ret, "1");

      System.out.println("Full Expression ok");

      // repeat same test by evaluating on one row at a time
      exprDesc = new exprNodeColumnDesc(String.class, "0");
      eval = ExprNodeEvaluatorFactory.get(exprDesc);

      // evaluate on row
      HiveObject ho = eval.evaluate(cr);
      exprDesc = new exprNodeColumnDesc(String.class, "col[1]");
      eval = ExprNodeEvaluatorFactory.get(exprDesc);
      ret = eval.evaluateToObject(ho);

      assertEquals(ret, "1");

      System.out.println("Nested Expression ok");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  
  public void testCompositeHiveObjectFilterOperator() throws Exception {
    try {

      exprNodeDesc f1 = new exprNodeColumnDesc(String.class, "0.col[2]");
      exprNodeDesc f2 = new exprNodeColumnDesc(String.class, "1.col[1]");
      exprNodeDesc f3 = new exprNodeColumnDesc(String.class, "2.col[0]");
      exprNodeDesc func1 = SemanticAnalyzer.getFuncExprNodeDesc("==", f1, f2);
      exprNodeDesc func2 = SemanticAnalyzer.getFuncExprNodeDesc("==", f2, f3);
      exprNodeDesc func3 = SemanticAnalyzer.getFuncExprNodeDesc("&&", func1, func2); 
      filterDesc filterCtx = new filterDesc(func3);
      
      // Configuration
      Operator<filterDesc> op = OperatorFactory.get(filterDesc.class);
      op.setConf(filterCtx);

      // runtime initialization
      op.initialize(null);

      // evaluate on row
      op.process(cr);

      Map<Enum, Long> results = op.getStats();
      assertEquals(results.get(FilterOperator.Counter.FILTERED), Long.valueOf(0));
      assertEquals(results.get(FilterOperator.Counter.PASSED), Long.valueOf(1));
      System.out.println("Filter Operator ok");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testCompositeHiveObjectSelectOperator() throws Exception {
    try {
      // col1
      exprNodeDesc exprDesc1 = new exprNodeColumnDesc(TypeInfo.getPrimitiveTypeInfo(String.class),
          "col[1]");

      // col2
      exprNodeDesc expr1 = new exprNodeColumnDesc(String.class, "col[0]");
      exprNodeDesc expr2 = new exprNodeConstantDesc("1");
      exprNodeDesc exprDesc2 = SemanticAnalyzer.getFuncExprNodeDesc("concat", expr1, expr2);

      // select operator to project these two columns
      ArrayList<exprNodeDesc> earr = new ArrayList<exprNodeDesc> ();
      earr.add(exprDesc1);
      earr.add(exprDesc2);
      selectDesc selectCtx = new selectDesc(earr);
      Operator<selectDesc> op = OperatorFactory.get(selectDesc.class);
      op.setConf(selectCtx);


      // collectOperator to look at the output of the select operator
      collectDesc cd = new collectDesc (Integer.valueOf(1));
      CollectOperator cdop = (CollectOperator) OperatorFactory.get(collectDesc.class);
      cdop.setConf(cd);
      ArrayList<Operator<? extends Serializable>> nextOp = new ArrayList<Operator<? extends Serializable>> ();
      nextOp.add(cdop);

      op.setChildOperators(nextOp);
      op.initialize(null);

      // evaluate on row
      op.process(r[0]);

      // analyze result
      HiveObject ho = cdop.retrieve();
      exprDesc1 = new exprNodeColumnDesc(TypeInfo.getPrimitiveTypeInfo(String.class), "0");
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(exprDesc1);
      Object ret = eval.evaluateToObject(ho);

      assertEquals(ret, "1");

      exprDesc1 = new exprNodeColumnDesc(TypeInfo.getPrimitiveTypeInfo(String.class), "1");
      eval = ExprNodeEvaluatorFactory.get(exprDesc1);
      ret = eval.evaluateToObject(ho);
      assertEquals(ret, "01");

      System.out.println("Select Operator ok");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }


  public void testLabeledCompositeObject() throws Exception {
    try {
      String [] fnames = {"key", "value"};
      LabeledCompositeHiveObject cr = new LabeledCompositeHiveObject(fnames);
      cr.addHiveObject(r[0]);
      cr.addHiveObject(r[1]);

      // get a evaluator for a simple field expression
      exprNodeDesc exprDesc = new exprNodeColumnDesc(String.class, "value.col[2]");
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(exprDesc);

      // evaluate on row
      Object ret = eval.evaluateToObject(cr);
      assertEquals(ret, "3");

      System.out.println("Labeled Composite full expression ok");

      // repeat same test by evaluating on one row at a time
      exprDesc = new exprNodeColumnDesc(String.class, "value");
      eval = ExprNodeEvaluatorFactory.get(exprDesc);

      // evaluate on row
      HiveObject ho = eval.evaluate(cr);
      exprDesc = new exprNodeColumnDesc(String.class, "col[2]");
      eval = ExprNodeEvaluatorFactory.get(exprDesc);
      ret = eval.evaluateToObject(ho);

      assertEquals(ret, "3");

      System.out.println("Labeled Composite nested Expression ok");


      exprDesc = new exprNodeColumnDesc(String.class, "invalid_field");
      eval = ExprNodeEvaluatorFactory.get(exprDesc);
      boolean gotException = false;
      try {
        ret = eval.evaluateToObject(cr);
      } catch (Exception e) {
        gotException = true;
      }
      assertEquals(gotException, true);

      System.out.println("Invalid field name check ok");

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  // TODO:
  // 1. test null hive objects
  // 2. test empty select expressions
}
