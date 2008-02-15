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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Deletes values from tables.
 */
public class DeleteCommand extends BasicCommand {
  public DeleteCommand(Writer o) {
    super(o);
  }

  private Text tableName;
  private Text rowKey;
  private List<String> columnList;

  public ReturnMsg execute(HBaseConfiguration conf) {
    if (columnList == null) {
      throw new IllegalArgumentException("Column list is null");
    }
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);

      if (!admin.tableExists(tableName)) {
        return new ReturnMsg(0, "'" + tableName + "'" + TABLE_NOT_FOUND);
      }

      HTable hTable = new HTable(conf, tableName);

      if (rowKey != null) {
        long lockID = hTable.startUpdate(rowKey);
        for (Text column : getColumnList(admin, hTable)) {
          hTable.delete(lockID, new Text(column));
        }
        hTable.commit(lockID);
      } else {
        admin.disableTable(tableName);
        for (Text column : getColumnList(admin, hTable)) {
          admin.deleteColumn(tableName, new Text(column));
        }
        admin.enableTable(tableName);
      }

      return new ReturnMsg(1, "Column(s) deleted successfully.");
    } catch (IOException e) {
      String[] msg = e.getMessage().split("[\n]");
      return new ReturnMsg(0, msg[0]);
    }
  }

  public void setTable(String tableName) {
    this.tableName = new Text(tableName);
  }

  public void setRow(String row) {
    this.rowKey = new Text(row);
  }

  /**
   * Sets the column list.
   * 
   * @param columnList
   */
  public void setColumnList(List<String> columnList) {
    this.columnList = columnList;
  }

  /**
   * @param admin
   * @param hTable
   * @return return the column list.
   */
  public Text[] getColumnList(HBaseAdmin admin, HTable hTable) {
    Text[] columns = null;
    try {
      if (columnList.contains("*")) {
        columns = hTable.getRow(new Text(this.rowKey)).keySet().toArray(
            new Text[] {});
      } else {
        List<Text> tmpList = new ArrayList<Text>();
        for (int i = 0; i < columnList.size(); i++) {
          Text column = null;
          if (columnList.get(i).contains(":"))
            column = new Text(columnList.get(i));
          else
            column = new Text(columnList.get(i) + ":");

          tmpList.add(column);
        }
        columns = tmpList.toArray(new Text[] {});
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return columns;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.DELETE;
  }
}
