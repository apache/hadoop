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

import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeIndexDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

public class TestExpressionEvaluator extends TestCase {

  // this is our row to test expressions on
  protected InspectableObject r;

  ArrayList<String> col1;
  TypeInfo col1Type;
  ArrayList<String> cola;
  TypeInfo colaType;
  ArrayList<Object> data;
  ArrayList<String> names;
  ArrayList<TypeInfo> typeInfos;
  TypeInfo dataType;
  
  public TestExpressionEvaluator() {
    col1 = new ArrayList<String> ();
    col1.add("0");
    col1.add("1");
    col1.add("2");
    col1.add("3");
    col1Type = TypeInfoFactory.getListTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfo(String.class));
    cola = new ArrayList<String> ();
    cola.add("a");
    cola.add("b");
    cola.add("c");
    colaType = TypeInfoFactory.getListTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfo(String.class));
    try {
      data = new ArrayList<Object>();
      data.add(col1);
      data.add(cola);
      names = new ArrayList<String>();
      names.add("col1");
      names.add("cola");
      typeInfos = new ArrayList<TypeInfo>();
      typeInfos.add(col1Type);
      typeInfos.add(colaType);
      dataType = TypeInfoFactory.getStructTypeInfo(names, typeInfos);
      
      r = new InspectableObject();
      r.o = data;
      r.oi = TypeInfoUtils.getStandardObjectInspectorFromTypeInfo(dataType);
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException (e);
    }
  }

  protected void setUp() {
  }

  public void testExprNodeColumnEvaluator() throws Throwable {
    try {
      // get a evaluator for a simple field expression
      exprNodeDesc exprDesc = new exprNodeColumnDesc(colaType, "cola");
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(exprDesc);

      // evaluate on row
      InspectableObject result = new InspectableObject();
      eval.evaluate(r.o, r.oi, result);
      assertEquals(result.o, cola);
      System.out.println("ExprNodeColumnEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testExprNodeFuncEvaluator() throws Throwable {
    try {
      // get a evaluator for a string concatenation expression
      exprNodeDesc col1desc = new exprNodeColumnDesc(col1Type, "col1");
      exprNodeDesc coladesc = new exprNodeColumnDesc(colaType, "cola");
      exprNodeDesc col11desc = new exprNodeIndexDesc(col1desc, new exprNodeConstantDesc(new Integer(1)));
      exprNodeDesc cola0desc = new exprNodeIndexDesc(coladesc, new exprNodeConstantDesc(new Integer(0)));
      exprNodeDesc func1 = SemanticAnalyzer.getFuncExprNodeDesc("concat", col11desc, cola0desc);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(func1);

      // evaluate on row
      InspectableObject result = new InspectableObject();
      eval.evaluate(r.o, r.oi, result);
      assertEquals(result.o, "1a");
      System.out.println("ExprNodeFuncEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testExprNodeConversionEvaluator() throws Throwable {
    try {
      // get a evaluator for a string concatenation expression
      exprNodeDesc col1desc = new exprNodeColumnDesc(col1Type, "col1");
      exprNodeDesc col11desc = new exprNodeIndexDesc(col1desc, new exprNodeConstantDesc(new Integer(1)));
      exprNodeDesc func1 = SemanticAnalyzer.getFuncExprNodeDesc(Double.class.getName(), col11desc);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(func1);

      // evaluate on row
      InspectableObject result = new InspectableObject();
      eval.evaluate(r.o, r.oi, result);
      assertEquals(result.o, Double.valueOf("1"));
      System.out.println("testExprNodeConversionEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static void measureSpeed(String expr, int times, ExprNodeEvaluator eval, InspectableObject input, Object standardOutput) throws HiveException {
    System.out.println("Evaluating " + expr + " for " + times + " times");
    // evaluate on row
    InspectableObject output = new InspectableObject(); 
    long start = System.currentTimeMillis();
    for (int i=0; i<times; i++) {
      eval.evaluate(input.o, input.oi, output);
      assertEquals(output.o, standardOutput);
    }
    long end = System.currentTimeMillis();
    System.out.println("Evaluation finished: " + String.format("%2.3f", (end - start)*0.001) + " seconds, " 
        + String.format("%2.3f", (end - start)*1000.0/times) + " seconds/million call.");
  }
  
  public void testExprNodeSpeed() throws Throwable {
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
      exprNodeDesc constant1 = new exprNodeConstantDesc(1);
      exprNodeDesc constant2 = new exprNodeConstantDesc(2);
      measureSpeed("concat(col1[1], cola[1])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat",
                  new exprNodeIndexDesc(new exprNodeColumnDesc(col1Type, "col1"), constant1), 
                  new exprNodeIndexDesc(new exprNodeColumnDesc(colaType, "cola"), constant1))),
          r,
          "1b");
      measureSpeed("concat(concat(col1[1], cola[1]), col1[2])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                  SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                      new exprNodeIndexDesc(new exprNodeColumnDesc(col1Type, "col1"), constant1), 
                      new exprNodeIndexDesc(new exprNodeColumnDesc(colaType, "cola"), constant1)),
                  new exprNodeIndexDesc(new exprNodeColumnDesc(col1Type, "col1"), constant2))),
          r,
          "1b2");
      measureSpeed("concat(concat(concat(col1[1], cola[1]), col1[2]), cola[2])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                  SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                      SemanticAnalyzer.getFuncExprNodeDesc("concat", 
                          new exprNodeIndexDesc(new exprNodeColumnDesc(col1Type, "col1"), constant1), 
                          new exprNodeIndexDesc(new exprNodeColumnDesc(colaType, "cola"), constant1)),
                      new exprNodeIndexDesc(new exprNodeColumnDesc(col1Type, "col1"), constant2)),
                  new exprNodeIndexDesc(new exprNodeColumnDesc(colaType, "cola"), constant2))),
          r,
          "1b2c");
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

}
