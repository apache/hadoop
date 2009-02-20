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

package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.tree.CommonTree;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.createFunctionDesc;

public class FunctionSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Log LOG =
    LogFactory.getLog("hive.ql.parse.FunctionSemanticAnalyzer");
  
  public FunctionSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }
  
  public void analyzeInternal(CommonTree ast, Context ctx) throws SemanticException {
    String functionName = ast.getChild(0).getText();
    String className = unescapeSQLString(ast.getChild(1).getText());
    createFunctionDesc desc = new createFunctionDesc(functionName, className);
    rootTasks.add(TaskFactory.get(new FunctionWork(desc), conf));
    LOG.info("analyze done");
  }
}
