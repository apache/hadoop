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
package org.apache.hadoop.hbase.shell;

import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.shell.algebra.Constants;
import org.apache.hadoop.hbase.shell.algebra.TestBooleanCondition;

/**
 * Binding variables, substitution variables test
 */
public class TestSubstitutionVariables extends TestCase {
  
  private String TABLE_NAME = "table_name";
  private String SUBSTITUTION_VARIABLE = "A";
  static HBaseConfiguration conf = new HBaseConfiguration();
  
  public void testSubstitution() {
    SubstituteCommand substitute = new SubstituteCommand(null);
    
    substitute.setKey(SUBSTITUTION_VARIABLE);
    substitute.setInput(TABLE_NAME);
    substitute.execute(conf);
    
    VariableRef ref = VariablesPool.get(SUBSTITUTION_VARIABLE).get(null);
    assertTrue(ref.getArgument().equals(TABLE_NAME));
  }
  
  public void testCombinedQueries() throws UnsupportedEncodingException {
    Writer out = new OutputStreamWriter(System.out, "UTF-8");
    SubstituteCommand substitute = new SubstituteCommand(out);
    
    substitute.setKey(SUBSTITUTION_VARIABLE);
    substitute.setInput(TABLE_NAME);
    substitute.execute(conf);
    
    substitute = new SubstituteCommand(out);
    substitute.setKey("B");
    substitute.setChainKey(SUBSTITUTION_VARIABLE);
    substitute.setOperation(Constants.RELATIONAL_SELECTION);
    substitute.setCondition(TestBooleanCondition.EXPRESSION_OR);
    substitute.execute(conf);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(TestSubstitutionVariables.class));
  }
  
}
