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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConnection;
import org.apache.hadoop.hbase.HConnectionManager;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * Represents the interface to an relational algebra operation like projection,
 * selection, join, group.
 */
public abstract class RelationalOperation implements Operation {
  protected JobConf jobConf;
  protected HConnection conn;
  protected HBaseAdmin admin;
  protected JobClient jobClient;
  protected HTableDescriptor desc;
  protected String input;
  protected String output;
  protected Map<String, String> condition;
  protected HTableDescriptor[] tables;
  protected Set<String> projSet = new HashSet<String>();

  /**
   * Constructor
   * 
   * @param conf
   * @param statements
   */
  public RelationalOperation(HBaseConfiguration conf,
      Map<String, String> statements) {
    this.jobConf = new JobConf(conf);
    this.conn = HConnectionManager.getConnection(conf);
    this.condition = statements;
    this.input = statements.get(Constants.CONFIG_INPUT);
    this.output = statements.get(Constants.CONFIG_OUTPUT);
    jobConf.setJobName("shell.mapred-" + +System.currentTimeMillis());
    desc = new HTableDescriptor(output);

    try {
      this.admin = new HBaseAdmin(conf);
      this.jobClient = new JobClient(jobConf);
      tables = conn.listTables();

      ClusterStatus cluster = jobClient.getClusterStatus();
      jobConf.setNumMapTasks(cluster.getMapTasks());
      jobConf.setNumReduceTasks(1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Gets the input table column descriptor[]
   * 
   * @return columns
   */
  public HColumnDescriptor[] getInputColumnDescriptor() {
    HColumnDescriptor[] columns = null;
    for (int i = 0; i < tables.length; i++) {
      if (tables[i].getName().equals(new Text(input))) {
        columns = tables[i].getFamilies().values().toArray(
            new HColumnDescriptor[] {});
        break;
      }
    }
    return columns;
  }

  /**
   * Convert HColumnDescriptor[] to String
   * 
   * @param columns
   * @return columns string
   */
  public String getColumnStringArray(HColumnDescriptor[] columns) {
    String result = "";
    for (int i = 0; i < columns.length; i++) {
      desc.addFamily(columns[i]);
      result += columns[i].getName() + " ";
    }
    return result;
  }

  /**
   * Creates the output table
   * 
   * @param columns
   * @param columnString
   * @throws IOException
   */
  public void outputTableCreate(HColumnDescriptor[] columns, String columnString)
      throws IOException {
    if (columnString == null) {
      for (int i = 0; i < columns.length; i++) {
        if (projSet.size() > 0) {
          desc.addFamily(columns[i]);
        } else {
          if (projSet.contains(columns[i].getName().toString())) {
            desc.addFamily(columns[i]);
          }
        }
      }
    } else {
      String[] cols = columnString.split(" ");
      for (int i = 0; i < cols.length; i++) {
        desc.addFamily(new HColumnDescriptor(cols[i]));
      }
    }

    admin.createTable(desc);
  }

  /**
   * Return the jobConf
   */
  public JobConf getConf() throws IOException, RuntimeException {
    return jobConf;
  }

  /**
   * @return projection conditions
   */
  public String getProjColumns() {
    return condition.get(Constants.RELATIONAL_PROJECTION);
  }

  /**
   * @return selection conditions
   */
  public String getExpression() {
    return condition.get(Constants.RELATIONAL_SELECTION);
  }

  /**
   * @return group conditions
   */
  public String getGroupColumns() {
    return condition.get(Constants.RELATIONAL_GROUP);
  }

  public Operation getOperation() {
    return this;
  }
}
