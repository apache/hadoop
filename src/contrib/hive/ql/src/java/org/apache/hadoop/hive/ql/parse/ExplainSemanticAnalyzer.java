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

import java.io.File;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.explainWork;

public class ExplainSemanticAnalyzer extends BaseSemanticAnalyzer {

  
  public ExplainSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  public void analyze(CommonTree ast, Context ctx) throws SemanticException {
    
    // Create a semantic analyzer for the query
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, (CommonTree)ast.getChild(0));
    sem.analyze((CommonTree)ast.getChild(0), ctx);
    
    boolean extended = false;
    if (ast.getChildCount() > 1) {
      extended = true;
    }
    
    ctx.setResFile(new Path(getTmpFileName()));
    
    rootTasks.add(TaskFactory.get(new explainWork(ctx.getResFile(),
                                                  sem.getRootTasks(),
                                                  ((CommonTree)ast.getChild(0)).toStringTree(),
                                                  extended), this.conf));
  }
}
