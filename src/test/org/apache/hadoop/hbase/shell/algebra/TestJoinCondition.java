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

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.hadoop.hbase.shell.algebra.generated.ExpressionParser;
import org.apache.hadoop.hbase.shell.algebra.generated.ParseException;

public class TestJoinCondition extends TestCase {
  ExpressionParser expressionParser;
  public static String EXPRESSION = "a.ROW = b.size BOOL a.length = b.length AND a.name = edward";
  
  public void testJoinCondition() {
    expressionParser = new ExpressionParser(EXPRESSION);
    try {
      expressionParser.joinExpressionParse();
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(TestJoinCondition.class));
  }
}
