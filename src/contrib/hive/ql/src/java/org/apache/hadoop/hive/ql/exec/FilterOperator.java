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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.ql.plan.filterDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.conf.Configuration;

/**
 * Filter operator implementation
 **/
public class FilterOperator extends Operator <filterDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  public static enum Counter {FILTERED, PASSED}
  transient private final LongWritable filtered_count, passed_count;
  transient private ExprNodeEvaluator conditionEvaluator;
  transient private InspectableObject conditionInspectableObject;  
  
  public FilterOperator () {
    super();
    filtered_count = new LongWritable();
    passed_count = new LongWritable();
    conditionInspectableObject = new InspectableObject();
  }

  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    try {
      this.conditionEvaluator = ExprNodeEvaluatorFactory.get(conf.getPredicate());
      statsMap.put(Counter.FILTERED, filtered_count);
      statsMap.put(Counter.PASSED, passed_count);
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void process(Object row, ObjectInspector rowInspector) throws HiveException {
    try {
      conditionEvaluator.evaluate(row, rowInspector, conditionInspectableObject);
      Boolean ret = (Boolean)(conditionInspectableObject.o);
      if (Boolean.TRUE.equals(ret)) {
        forward(row, rowInspector);
        passed_count.set(passed_count.get()+1);
      } else {
        filtered_count.set(filtered_count.get()+1);
      }
    } catch (ClassCastException e) {
      e.printStackTrace();
      throw new HiveException("Non Boolean return Object type: " +
          conditionInspectableObject.o.getClass().getName());
    }
  }
}
