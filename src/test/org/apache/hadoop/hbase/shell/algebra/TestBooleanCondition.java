/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.shell.algebra;

import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.shell.algebra.generated.ExpressionParser;
import org.apache.hadoop.hbase.shell.algebra.generated.ParseException;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

/**
 * Test boolean expression
 */
public class TestBooleanCondition extends TestCase {
  ExpressionParser expression;
  public static String EXPRESSION_OR = "key:2 = value2 OR key:3 = value1";
  public static String EXPRESSION_AND = "key:2 = value2 AND key:3 = value1";
  public static String EXPRESSION = "key:3 > 1";
  public static String TERM_FILTER = "key:4 > 100 AND key:4 !! 110|120|130|140|150";

  Text[] keys = { new Text("key:1"), new Text("key:2"), new Text("key:3"), new Text("key:4") };

  ImmutableBytesWritable[] values = {
      new ImmutableBytesWritable("value1".getBytes()),
      new ImmutableBytesWritable("value2".getBytes()),
      new ImmutableBytesWritable("3".getBytes()),
      new ImmutableBytesWritable("150".getBytes())
  };

  public void testCheckConstraints() throws UnsupportedEncodingException {
    MapWritable data = new MapWritable();
    for (int i = 0; i < keys.length; i++) {
      data.put(keys[i], values[i]);
    }

    try {
      expression = new ExpressionParser(EXPRESSION_OR);
      expression.booleanExpressionParse();
      assertTrue(expression.checkConstraints(data));

      expression = new ExpressionParser(EXPRESSION_AND);
      expression.booleanExpressionParse();
      assertFalse(expression.checkConstraints(data));

      expression = new ExpressionParser(EXPRESSION);
      expression.booleanExpressionParse();
      assertTrue(expression.checkConstraints(data));
      
      expression = new ExpressionParser(TERM_FILTER);
      expression.booleanExpressionParse();
      assertFalse(expression.checkConstraints(data));
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(TestBooleanCondition.class));
  }

}
