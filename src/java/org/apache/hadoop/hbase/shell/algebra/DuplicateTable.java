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
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.mapred.IdentityTableMap;
import org.apache.hadoop.hbase.mapred.IdentityTableReduce;
import org.apache.hadoop.mapred.JobConf;

/**
 * Duplicates Table. R1 to R3 in O(N)
 */
public class DuplicateTable extends RelationalOperation {
  public DuplicateTable(HBaseConfiguration conf, Map<String, String> condition) {
    super(conf, condition);
  }

  @Override
  public JobConf getConf() throws IOException, RuntimeException {
    HColumnDescriptor[] columns = getInputColumnDescriptor();
    outputTableCreate(columns, null);

    IdentityTableMap.initJob(input, getColumnStringArray(columns),
        IdentityTableMap.class, jobConf);
    IdentityTableReduce.initJob(output, IdentityTableReduce.class, jobConf);

    return jobConf;
  }
}
