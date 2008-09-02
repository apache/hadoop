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
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes;

public class TestExpressionEvaluator extends TestCase {

  // this is our row to test expressions on
  protected HiveObject r;

  protected void setUp() {
    ArrayList<String> data = new ArrayList<String> ();
    data.add("0");
    data.add("1");
    data.add("2");
    data.add("3");
    ColumnSet cs = new ColumnSet(data);
    try {
      r = new TableHiveObject(cs, new columnsetSerDe());
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
  }


  public void testExprNodeColumnEvaluator() throws Exception {
    try {
      // get a evaluator for a simple field expression
      exprNodeDesc exprDesc = new exprNodeColumnDesc(String.class, "col[1]");
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(exprDesc);

      // evaluate on row
      Object ret = eval.evaluateToObject(r);
      assertEquals(ret, "1");
      System.out.println("ExprNodeColumnEvaluator ok");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testExprNodeFuncEvaluator() throws Exception {
    try {
      // get a evaluator for a string concatenation expression
      exprNodeDesc col0 = new exprNodeColumnDesc(String.class, "col[0]");
      exprNodeDesc col1 = new exprNodeColumnDesc(String.class, "col[1]");
      exprNodeDesc func1 = SemanticAnalyzer.getFuncExprNodeDesc("concat", col0, col1);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(func1);

      // evaluate on row
      Object ret = eval.evaluateToObject(r);
      assertEquals(ret, "01");
      System.out.println("ExprNodeFuncEvaluator ok");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testExprNodeConversionEvaluator() throws Exception {
    try {
      // get a evaluator for a string concatenation expression
      exprNodeDesc col0 = new exprNodeConstantDesc(String.class, null);
      exprNodeDesc func1 = SemanticAnalyzer.getFuncExprNodeDesc("java.lang.double", col0);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(func1);

      // evaluate on row
      Object ret = eval.evaluateToObject(r);
      assertEquals(ret, null);
      System.out.println("testExprNodeConversionEvaluator ok");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static void measureSpeed(String expr, int times, ExprNodeEvaluator eval, HiveObject input, Object output) throws HiveException {
    System.out.println("Evaluating " + expr + " for " + times + " times");
    // evaluate on row
    long start = System.currentTimeMillis();
    for (int i=0; i<times; i++) {
      Object ret = eval.evaluateToObject(input);
      assertEquals(ret, output);
    }
    long end = System.currentTimeMillis();
    System.out.println("Evaluation finished: " + String.format("%2.3f", (end - start)*0.001) + " seconds, " 
        + String.format("%2.3f", (end - start)*1000.0/times) + " seconds/million call.");
  }
  
  public void testExprNodeSpeed() throws Exception {
    try {
      int basetimes = 100000;
      measureSpeed("1 + 2",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("+", 
                  new exprNodeConstantDesc(1), 
                  new exprNodeConstantDesc(2))),
          r,
          Integer.valueOf(1 + 2));
      measureSpeed("1 + 2 - 3",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("-", 
                  SemanticAnalyzer.getFuncExprNodeDesc("+",
                      new exprNodeConstantDesc(1), 
                      new exprNodeConstantDesc(2)),
                  new exprNodeConstantDesc(3))),
          r,
          Integer.valueOf(1 + 2 - 3));
      measureSpeed("1 + 2 - 3 + 4",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("+",
                  SemanticAnalyzer.getFuncExprNodeDesc("-", 
                      SemanticAnalyzer.getFuncExprNodeDesc("+",
                          new exprNodeConstantDesc(1), 
                          new exprNodeConstantDesc(2)),
                      new exprNodeConstantDesc(3)),
                  new exprNodeConstantDesc(4))),                      
          r,
          Integer.valueOf(1 + 2 - 3 + 4));
      measureSpeed("concat(\"1\", \"2\")",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                  new exprNodeConstantDesc("1"), 
                  new exprNodeConstantDesc("2"))),
          r,
          "12");
      measureSpeed("concat(concat(\"1\", \"2\"), \"3\")",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                  SemanticAnalyzer.getFuncExprNodeDesc("concat",
                      new exprNodeConstantDesc("1"), 
                      new exprNodeConstantDesc("2")),
                  new exprNodeConstantDesc("3"))),
          r,
          "123");
      measureSpeed("concat(concat(concat(\"1\", \"2\"), \"3\"), \"4\")",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                    SemanticAnalyzer.getFuncExprNodeDesc("concat",
                        new exprNodeConstantDesc("1"), 
                        new exprNodeConstantDesc("2")),
                    new exprNodeConstantDesc("3")),
                new exprNodeConstantDesc("4"))),
          r,
          "1234");
      measureSpeed("concat(col[0], col[1])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                  new exprNodeColumnDesc(String.class, "col[0]"), 
                  new exprNodeColumnDesc(String.class, "col[1]"))),
          r,
          "01");
      measureSpeed("concat(concat(col[0], col[1]), col[2])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                  SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                      new exprNodeColumnDesc(String.class, "col[0]"), 
                      new exprNodeColumnDesc(String.class, "col[1]")),
                  new exprNodeColumnDesc(String.class, "col[2]"))),
          r,
          "012");
      measureSpeed("concat(concat(concat(col[0], col[1]), col[2]), col[3])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                  SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                      SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                          new exprNodeColumnDesc(String.class, "col[0]"), 
                          new exprNodeColumnDesc(String.class, "col[1]")),
                      new exprNodeColumnDesc(String.class, "col[2]")),
                  new exprNodeColumnDesc(String.class, "col[3]"))),
          r,
          "0123");
      
      
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

}
