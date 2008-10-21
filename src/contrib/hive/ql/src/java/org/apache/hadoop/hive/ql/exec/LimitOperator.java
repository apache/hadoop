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
import org.apache.hadoop.hive.ql.plan.limitDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.conf.Configuration;

/**
 * Limit operator implementation
 * Limits a subobject and passes that on.
 **/
public class LimitOperator extends Operator<limitDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  
  transient protected int limit;
  transient protected int currCount;

  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    limit = conf.getLimit();
    currCount = 0;
  }

  public void process(Object row, ObjectInspector rowInspector) throws HiveException {
    if (currCount < limit) {
      forward(row, rowInspector);
      currCount++;
    }
    else
      setDone(true);
  }
}
