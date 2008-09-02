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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.io.*;

/**
 * Reduce Sink Operator sends output to the reduce stage
 **/
public class ReduceSinkOperator extends TerminalOperator <reduceSinkDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ExprNodeEvaluator[] keyEval;
  transient protected ExprNodeEvaluator[] valueEval;
  transient WritableComparableHiveObject wcho;
  transient WritableHiveObject who;
  transient boolean keyIsSingleton, valueIsSingleton;

  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    try {
      keyEval = new ExprNodeEvaluator[conf.getKeyCols().size()];
      int i=0;
      for(exprNodeDesc e: conf.getKeyCols()) {
        keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }
      keyIsSingleton = false; //(i == 1);

      valueEval = new ExprNodeEvaluator[conf.getValueCols().size()];
      i=0;
      for(exprNodeDesc e: conf.getValueCols()) {
        valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }
      valueIsSingleton = false; //(i == 1);

      // TODO: Use NaiiveSerializer for now 
      // Once we make sure CompositeHiveObject.getJavaObject() returns a Java List,
      // we will use MetadataTypedSerDe to serialize the data, instead of using
      // NaiiveSerializer.
      int tag = conf.getTag();
      if(tag == -1) {
        who = new NoTagWritableHiveObject(null, new NaiiveSerializer());
        wcho = new NoTagWritableComparableHiveObject(null, new NaiiveSerializer());
        l4j.info("Using tag = -1");
      } else {
        l4j.info("Using tag = " + tag);
        who = new WritableHiveObject(tag, null, new NaiiveSerializer());
        wcho = new WritableComparableHiveObject(tag, null, new NaiiveSerializer());
      }

      // Set the number of key fields to be used in the partitioner.
      WritableComparableHiveObject.setNumPartitionFields(conf.getNumPartitionFields());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void process(HiveObject r) throws HiveException {
    if(keyIsSingleton) {
      wcho.setHo(keyEval[0].evaluate(r));
    } else {
      CompositeHiveObject nr = new CompositeHiveObject (keyEval.length);
      for(ExprNodeEvaluator e: keyEval) {
        nr.addHiveObject(e.evaluate(r));
      }
      wcho.setHo(nr);
    }

    if(valueIsSingleton) {
      who.setHo(valueEval[0].evaluate(r));
    } else {
      CompositeHiveObject nr = new CompositeHiveObject (valueEval.length);
      for(ExprNodeEvaluator e: valueEval) {
        nr.addHiveObject(e.evaluate(r));
      }
      who.setHo(nr);
    }

    try {
      out.collect(wcho, who);
    } catch (IOException e) {
      throw new HiveException (e);
    }
  }
}
