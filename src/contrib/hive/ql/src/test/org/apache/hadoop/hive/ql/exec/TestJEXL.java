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

import org.apache.commons.jexl.Expression;
import org.apache.commons.jexl.ExpressionFactory;
import org.apache.commons.jexl.JexlContext;
import org.apache.commons.jexl.JexlHelper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;

public class TestJEXL extends TestCase {

  public void testJEXL() throws Exception {

    Integer a = Integer.valueOf (5);
    Integer b = Integer.valueOf (8);
    JexlContext jc = JexlHelper.createContext();
    jc.getVars().put("a", a);
    jc.getVars().put("b", b);
    try {
      Expression e = ExpressionFactory.createExpression("a+b");
      Object o = e.evaluate(jc);
      assertEquals(o, Long.valueOf(13));
      System.out.println("JEXL library test ok");
    } catch (Exception e) {
      e.printStackTrace();
      throw (e);
    }
  }

  private static void measureSpeed(String expr, int times, Expression eval, JexlContext input, Object output) throws Exception {
    System.out.println("Evaluating " + expr + " for " + times + " times");
    // evaluate on row
    long start = System.currentTimeMillis();
    for (int i=0; i<times; i++) {
      Object ret = eval.evaluate(input);
      // System.out.println("" + ret.getClass() + " " + ret);
      assertEquals(ret, output);
    }
    long end = System.currentTimeMillis();
    System.out.println("Evaluation finished: " + String.format("%2.3f", (end - start)*0.001) + " seconds, " 
        + String.format("%2.3f", (end - start)*1000.0/times) + " seconds/million call.");
  }
  
  public void testJEXLSpeed() throws Exception {
    try {
      int basetimes = 100000;

      JexlContext jc = JexlHelper.createContext();
      jc.getVars().put("__udf__concat", FunctionRegistry.getUDFClass("concat").newInstance());
      
      measureSpeed("1 + 2",
          basetimes * 100,
          ExpressionFactory.createExpression("1 + 2"),
          jc,
          Long.valueOf(1 + 2));
  
      measureSpeed("__udf__concat.evaluate(\"1\", \"2\")",
          basetimes * 10,
          ExpressionFactory.createExpression("__udf__concat.evaluate(\"1\", \"2\")"),
          jc,
          "12");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
  
  public void testExprNodeFuncEvaluator() throws Exception {
  }  
}
