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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.serde.SerDeField;

public class ExprNodeFieldEvaluator extends ExprNodeEvaluator {

  protected exprNodeFieldDesc desc;
  transient ExprNodeEvaluator evaluator;
  transient SerDeField field;
  
  public ExprNodeFieldEvaluator(exprNodeFieldDesc desc) {
    this.desc = desc;
    evaluator = ExprNodeEvaluatorFactory.get(desc.getDesc());
  }


  public Object evaluateToObject(HiveObject row)  throws HiveException {
    return evaluate(row).getJavaObject();
  }

  public HiveObject evaluate(HiveObject row) throws HiveException {
    HiveObject ho = evaluator.evaluate(row);
    if (field == null) {
      field = ho.getFieldFromExpression(desc.getFieldName());
    }
    return ho.get(field);
  }
}
