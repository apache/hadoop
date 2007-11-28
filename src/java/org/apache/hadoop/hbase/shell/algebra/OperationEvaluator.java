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
package org.apache.hadoop.hbase.shell.algebra;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.shell.VariableRef;
import org.apache.hadoop.hbase.shell.VariablesPool;
import org.apache.hadoop.mapred.JobConf;

/**
 * Each algebra operation can be evaluated one of several different algorithms.
 * 
 * So, It should be query executor/optimizer later. And It will become the core
 * module that regulates the query-performance of Hbase Shell.
 * 
 * @see <a
 *      href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseShell/Executor">Intergrated
 *      query executor architecture</a>
 */
public class OperationEvaluator {
  private HBaseConfiguration conf;
  Map<String, String> condition = new HashMap<String, String>();

  /** Constructor */
  public OperationEvaluator(HBaseConfiguration conf, String chainKey, String output) {
    this.conf = conf;
    String chain = chainKey;
    String input = null;

    while (chain != null) {
      for (Map.Entry<String, VariableRef> e : VariablesPool.get(chain).entrySet()) {
        if (e.getKey() == null) {
          input = e.getValue().getArgument();
        } else {
          condition.put(e.getValue().getOperation(), e.getValue()
              .getArgument());
        }
        chain = e.getKey();
      }
    }
    condition.put(Constants.CONFIG_INPUT, input);
    condition.put(Constants.CONFIG_OUTPUT, output);
  }


  /**
   * Returns the job configuration object for statements type
   * 
   * @return JobConf
   * @throws IOException
   * @throws RuntimeException
   */
  public JobConf getJobConf() throws IOException, RuntimeException {
    RelationalOperation operation;
    if (condition.containsKey(Constants.RELATIONAL_SELECTION)) {
      operation = new Selection(conf, condition);
    } else if (condition.containsKey(Constants.RELATIONAL_PROJECTION)) {
      operation = new Projection(conf, condition);
    } else if (condition.containsKey(Constants.RELATIONAL_JOIN)) {
      operation = new IndexJoin(conf, condition);
    } else {
      operation = new DuplicateTable(conf, condition);
    }

    return operation.getOperation().getConf();
  }
}
