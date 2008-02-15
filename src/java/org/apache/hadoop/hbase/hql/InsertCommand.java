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
package org.apache.hadoop.hbase.hql;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.MasterNotRunningException;

/**
 * Inserts values into tables.
 */
public class InsertCommand extends BasicCommand {
  private Text tableName;
  private List<String> columnfamilies;
  private List<String> values;
  private String rowKey;
  private String timestamp = null;

  public InsertCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(HBaseConfiguration conf) {
    if (tableName == null || values == null || rowKey == null)
      return new ReturnMsg(0, "Syntax error : Please check 'Insert' syntax.");
    
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (!admin.tableExists(tableName)) {
        return new ReturnMsg(0, "'" + tableName + "'" + TABLE_NOT_FOUND);
      }
      
      if (columnfamilies.size() != values.size())
        return new ReturnMsg(0,
            "Mismatch between values list and columnfamilies list.");

      try {
        HTable table = new HTable(conf, tableName);
        long lockId = table.startUpdate(getRow());

        for (int i = 0; i < values.size(); i++) {
          Text column = null;
          if (getColumn(i).toString().contains(":"))
            column = getColumn(i);
          else
            column = new Text(getColumn(i) + ":");
          table.put(lockId, column, getValue(i));
        }

        if(timestamp != null) 
          table.commit(lockId, Long.parseLong(timestamp));
        else
          table.commit(lockId);

        return new ReturnMsg(1, "1 row inserted successfully.");
      } catch (IOException e) {
        String[] msg = e.getMessage().split("[\n]");
        return new ReturnMsg(0, msg[0]);
      } 
    } catch (MasterNotRunningException e) {
      return new ReturnMsg(0, "Master is not running!");
    }
  }

  public void setTable(String table) {
    this.tableName = new Text(table);
  }

  public void setColumnfamilies(List<String> columnfamilies) {
    this.columnfamilies = columnfamilies;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  public void setRow(String row) {
    this.rowKey = row;
  }

  public Text getRow() {
    return new Text(this.rowKey);
  }

  public Text getColumn(int i) {
    return new Text(this.columnfamilies.get(i));
  }

  public byte[] getValue(int i) {
    return this.values.get(i).getBytes();
  }
  
  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }
  
  @Override
  public CommandType getCommandType() {
    return CommandType.INSERT;
  }
}
