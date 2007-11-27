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
package org.apache.hadoop.hbase.shell;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConnection;
import org.apache.hadoop.hbase.HConnectionManager;
import org.apache.hadoop.io.Text;

/**
 * Alters tables.
 */
public class AlterCommand extends SchemaModificationCommand {
  public enum OperationType {ADD, DROP, CHANGE, NOOP}
  private OperationType operationType = OperationType.NOOP;
  private Map<String, Map<String, Object>> columnSpecMap =
    new HashMap<String, Map<String, Object>>();
  private String tableName;
  private String column; // column to be dropped

  public AlterCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(HBaseConfiguration conf) {
    try {
      HConnection conn = HConnectionManager.getConnection(conf);
      if (!conn.tableExists(new Text(this.tableName))) {
        return new ReturnMsg(0, "'" + this.tableName + "' Table not found");
      }
      
      HBaseAdmin admin = new HBaseAdmin(conf);
      Set<String> columns = null;
      HColumnDescriptor columnDesc = null;
      switch (operationType) {
      case ADD:
        disableTable(admin, tableName);
        columns = columnSpecMap.keySet();
        for (String c : columns) {
          columnDesc = getColumnDescriptor(c, columnSpecMap.get(c));
          println("Adding " + c + " to " + tableName +
            "... Please wait.");
          admin.addColumn(new Text(tableName), columnDesc);
        }
        enableTable(admin, tableName);
        break;
      case DROP:
        disableTable(admin, tableName);
        println("Dropping " + column + " from " + tableName +
          "... Please wait.");
        column = appendDelimiter(column);
        admin.deleteColumn(new Text(tableName), new Text(column));
        enableTable(admin, tableName);
        break;
      case CHANGE:
        // Not yet supported
        return new ReturnMsg(0, "" + operationType + " is not yet supported.");
      case NOOP:
        return new ReturnMsg(0, "Invalid operation type.");
      }
      return new ReturnMsg(0, "Table altered successfully.");
    } catch (Exception e) {
      return new ReturnMsg(0, extractErrMsg(e));
    }
  }

  private void disableTable(HBaseAdmin admin, String t) throws IOException {
    println("Disabling " + t + "... Please wait.");
    admin.disableTable(new Text(t));
  }

  private void enableTable(HBaseAdmin admin, String t) throws IOException {
    println("Enabling " + t + "... Please wait.");
    admin.enableTable(new Text(t));
  }

  /**
   * Sets the table to be altered.
   * 
   * @param t Table to be altered.
   */
  public void setTable(String t) {
    this.tableName = t;
  }

  /**
   * Adds a column specification.
   * 
   * @param columnSpec Column specification
   */
  public void addColumnSpec(String c, Map<String, Object> columnSpec) {
    columnSpecMap.put(c, columnSpec);
  }

  /**
   * Sets the column to be dropped. Only applicable to the DROP operation.
   * 
   * @param c Column to be dropped.
   */
  public void setColumn(String c) {
    this.column = c;
  }

  /**
   * Sets the operation type of this alteration.
   * 
   * @param operationType Operation type
   * @see OperationType
   */
  public void setOperationType(OperationType operationType) {
    this.operationType = operationType;
  }
  
  @Override
  public CommandType getCommandType() {
    return CommandType.DDL;
  }
}