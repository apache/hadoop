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

import java.util.*;
import java.io.*;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.collectDesc;
import org.apache.hadoop.conf.Configuration;

/**
 * Buffers rows emitted by other operators
 **/
public class CollectOperator extends Operator <collectDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ArrayList<HiveObject> objList;
  transient int maxSize;

  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    objList = new ArrayList<HiveObject> ();
    maxSize = conf.getBufferSize().intValue();
  }

  public void process(HiveObject r) throws HiveException {
    if(objList.size() < maxSize) {
      objList.add(r);
    }
    forward(r);
  }
  
  public HiveObject retrieve() {
    if(objList.isEmpty())
      return null;
    return objList.remove(0);
  }

}
