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
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableOutputCollector;
import org.apache.hadoop.hbase.shell.algebra.generated.ExpressionParser;
import org.apache.hadoop.hbase.shell.algebra.generated.ParseException;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * An index join exploits the existence of an row index for one of the relations
 * used in the join to find matching rows more quickly.
 * 
 * Index join (using R2 row index) takes time O(i+m)/map function number.
 */
public class IndexJoinMap extends TableMap {
  ExpressionParser expressionParser;
  private String secondRelation;
  public static final String JOIN_EXPRESSION = "shell.mapred.join.expression";
  public static final String SECOND_RELATION = "shell.mapred.join.second.relation";
  public static final String FIRST_COLUMNS = "shell.mapred.first.columns";
  private Text[] first_columns;

  /** constructor */
  public IndexJoinMap() {
    super();
  }

  /**
   * @param firstRelation R1
   * @param secondRelation R2
   * @param firstColumns (A 1,A 2,...,A n)
   * @param secondColumns (B~1~,B~2~,...,B~m~)
   * @param joinExpression join condition expression
   * @param mapper mapper class
   * @param job jobConf
   */
  public static void initJob(String firstRelation, String secondRelation,
      String firstColumns, String secondColumns, String joinExpression,
      Class<? extends TableMap> mapper, JobConf job) {
    initJob(firstRelation, firstColumns, mapper, job);
    job.set(JOIN_EXPRESSION, joinExpression);
    job.set(SECOND_RELATION, secondRelation);
    job.set(FIRST_COLUMNS, firstColumns);
  }

  /** {@inheritDoc} */
  @Override
  public void configure(JobConf job) {
    super.configure(job);
    secondRelation = job.get(SECOND_RELATION, "");
    String[] cols = job.get(FIRST_COLUMNS, "").split(" ");
    first_columns = new Text[cols.length];
    for (int i = 0; i < cols.length; i++) {
      first_columns[i] = new Text(cols[i]);
    }

    expressionParser = new ExpressionParser(job.get(JOIN_EXPRESSION, ""));
    try {
      expressionParser.joinExpressionParse();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void map(HStoreKey key, MapWritable value,
      TableOutputCollector output, Reporter reporter) throws IOException {
    Text tKey = key.getRow();
    try {
      MapWritable appendValue = expressionParser.getJoinColumns(value,
          first_columns.length, secondRelation);

      if (appendValue.size() != 0) {
        value.putAll(appendValue);
        if (expressionParser.checkConstraints(value)) {
          output.collect(tKey, value);
        }
      }
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
}
