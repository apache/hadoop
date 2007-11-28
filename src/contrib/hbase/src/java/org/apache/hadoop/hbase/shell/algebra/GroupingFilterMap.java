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

import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.mapred.GroupingTableMap;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableOutputCollector;
import org.apache.hadoop.hbase.shell.algebra.generated.ExpressionParser;
import org.apache.hadoop.hbase.shell.algebra.generated.ParseException;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * Extract grouping columns from filtered records.
 */
public class GroupingFilterMap extends GroupingTableMap {
  ExpressionParser expressionParser;
  public static final String EXPRESSION = "shell.mapred.filtertablemap.exps";

  public static void initJob(String table, String columns, String groupColumns,
      String expression, Class<? extends TableMap> mapper, JobConf job) {
    initJob(table, columns, mapper, job);
    job.set(GROUP_COLUMNS, groupColumns);
    job.set(EXPRESSION, expression);
  }

  /** {@inheritDoc} */
  @Override
  public void configure(JobConf job) {
    super.configure(job);
    String[] cols = job.get(GROUP_COLUMNS, "").split(" ");
    m_columns = new Text[cols.length];
    for (int i = 0; i < cols.length; i++) {
      m_columns[i] = new Text(cols[i]);
    }
    expressionParser = new ExpressionParser(job.get(EXPRESSION, ""));
    try {
      expressionParser.booleanExpressionParse();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public void map(@SuppressWarnings("unused")
  HStoreKey key, MapWritable value, TableOutputCollector output,
      @SuppressWarnings("unused")
      Reporter reporter) throws IOException {
    byte[][] keyVals = extractKeyValues(value);
    if (keyVals != null) {
      Text tKey = createGroupKey(keyVals);

      try {
        if (expressionParser.checkConstraints(value)) {
          output.collect(tKey, value);
        }
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
