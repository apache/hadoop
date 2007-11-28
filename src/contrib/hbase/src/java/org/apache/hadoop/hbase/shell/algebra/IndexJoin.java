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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * Perform a index join using MapReduce.
 */
public class IndexJoin extends RelationalOperation {
  public IndexJoin(HBaseConfiguration conf, Map<String, String> condition) {
    super(conf, condition);
  }

  @Override
  public JobConf getConf() throws IOException, RuntimeException {
    String secondRelation = condition.get(Constants.JOIN_SECOND_RELATION);

    HColumnDescriptor[] firstColumns = null;
    HColumnDescriptor[] secondColumns = null;
    for (int i = 0; i < tables.length; i++) {
      if (tables[i].getName().equals(new Text(input))) {
        firstColumns = tables[i].getFamilies().values().toArray(
            new HColumnDescriptor[] {});
      } else if (tables[i].getName().equals(new Text(secondRelation))) {
        secondColumns = tables[i].getFamilies().values().toArray(
            new HColumnDescriptor[] {});
      }
    }

    String firstColumnsStr = "";
    String secondColumnsStr = "";

    for (int i = 0; i < firstColumns.length; i++) {
      desc.addFamily(firstColumns[i]);
      firstColumnsStr += firstColumns[i].getName() + " ";
    }

    for (int i = 0; i < secondColumns.length; i++) {
      desc.addFamily(secondColumns[i]);
      secondColumnsStr += secondColumns[i].getName() + " ";
    }

    admin.createTable(desc); // create output table.

    IndexJoinMap.initJob(input, secondRelation, firstColumnsStr,
        secondColumnsStr, condition.get(Constants.RELATIONAL_JOIN),
        IndexJoinMap.class, jobConf);
    IndexJoinReduce.initJob(output, IndexJoinReduce.class, jobConf);

    return jobConf;
  }
}
