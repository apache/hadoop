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

import java.io.*;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.conf.Configuration;

/**
 * Select operator implementation
 **/
public class SelectOperator extends Operator <selectDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ExprNodeEvaluator[] eval;

  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    try {
      eval = new ExprNodeEvaluator[conf.getColList().size()];
      int i=0;
      for(exprNodeDesc e: conf.getColList()) {
        eval[i++] = ExprNodeEvaluatorFactory.get(e);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void process(HiveObject r) throws HiveException {
    CompositeHiveObject nr = new CompositeHiveObject (eval.length);
    for(ExprNodeEvaluator e: eval) {
      HiveObject ho = e.evaluate(r);
      nr.addHiveObject(ho);
    }
    forward(nr);
  }
}
