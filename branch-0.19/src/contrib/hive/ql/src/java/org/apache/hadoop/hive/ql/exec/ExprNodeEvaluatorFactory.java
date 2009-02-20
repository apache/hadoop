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

import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeIndexDesc;

public class ExprNodeEvaluatorFactory {
  
  public ExprNodeEvaluatorFactory() {}

  public static ExprNodeEvaluator get(exprNodeDesc desc) {
    // Constant node
    if (desc instanceof exprNodeConstantDesc) {
      return new ExprNodeConstantEvaluator((exprNodeConstantDesc)desc);
    }
    // Column-reference node, e.g. a column in the input row
    if (desc instanceof exprNodeColumnDesc) {
      return new ExprNodeColumnEvaluator((exprNodeColumnDesc)desc);
    }
    // Function node, e.g. an operator or a UDF node
    if (desc instanceof exprNodeFuncDesc) {
      return new ExprNodeFuncEvaluator((exprNodeFuncDesc)desc);
    }
    // Field node, e.g. get a.myfield1 from a
    if (desc instanceof exprNodeFieldDesc) {
      return new ExprNodeFieldEvaluator((exprNodeFieldDesc)desc);
    }
    // Index node, e.g. get a[index] from a
    if (desc instanceof exprNodeIndexDesc) {
      return new ExprNodeIndexEvaluator((exprNodeIndexDesc)desc);
    }
    // Null node, a constant node with value NULL and no type information 
    if (desc instanceof exprNodeNullDesc) {
      return new ExprNodeNullEvaluator((exprNodeNullDesc)desc);
    }

    throw new RuntimeException("Cannot find ExprNodeEvaluator for the exprNodeDesc = " + desc);
  }
}
