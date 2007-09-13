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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;

/**
 * Selects values from tables.
 * 
 * TODO: INTO FILE is not yet implemented.
 */
public class SelectCommand extends BasicCommand {
  
  private Text tableName;
  private Text rowKey = new Text("");
  private List<String> columns;
  private long timestamp;
  private int limit;
  private int version;
  private boolean whereClause = false;

  public ReturnMsg execute(Configuration conf) {
    if (this.tableName.equals("") || this.rowKey == null ||
        this.columns.size() == 0) {
      return new ReturnMsg(0, "Syntax error : Please check 'Select' syntax.");
    } 
    
    try {
      HTable table = new HTable(conf, this.tableName);
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (this.whereClause) {
        compoundWherePrint(table, admin);
      } else {
        scanPrint(table, admin);
      }
      return new ReturnMsg(1, "Successfully print out the selected data.");
    } catch (IOException e) {
      String[] msg = e.getMessage().split("[,]");
      return new ReturnMsg(0, msg[0]);
    }
  }

  private void compoundWherePrint(HTable table, HBaseAdmin admin) {
    try {
      if (this.version != 0) {
        byte[][] result = null;
        Text[] cols = getColumns(admin);
        for (int i = 0; i < cols.length; i++) {
          if (this.timestamp == 0) {
            result = table.get(this.rowKey, cols[i], this.timestamp, this.version);
          } else {
            result = table.get(this.rowKey, cols[i], this.version);
          }

          ConsoleTable.selectHead();
          for (int ii = 0; ii < result.length; ii++) {
            ConsoleTable.printLine(i, this.rowKey.toString(), cols[i].toString(),
                new String(result[ii], HConstants.UTF8_ENCODING));
          }
          ConsoleTable.selectFoot();
        }
      } else {
        int count = 0;
        ConsoleTable.selectHead();
        
        for (Map.Entry<Text, byte[]> entry : table.getRow(this.rowKey).entrySet()) {
          byte[] value = entry.getValue();
          String cellData = new String(value, HConstants.UTF8_ENCODING);

          if (entry.getKey().equals(HConstants.COL_REGIONINFO)) {
            DataInputBuffer inbuf = new DataInputBuffer();
            HRegionInfo info = new HRegionInfo();
            inbuf.reset(value, value.length);
            info.readFields(inbuf);
            cellData = String.valueOf(info.getRegionId());
          }

          if (columns.contains(entry.getKey().toString()) || columns.contains("*")) {
            ConsoleTable.printLine(count, this.rowKey.toString(), entry.getKey()
                .toString(), cellData);
            count++;
          }
        }
        ConsoleTable.selectFoot();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void scanPrint(HTable table, HBaseAdmin admin) {
    HScannerInterface scan = null;
    try {
      if (this.timestamp == 0) {
        scan = table.obtainScanner(getColumns(admin), this.rowKey);
      } else {
        scan = table.obtainScanner(getColumns(admin), this.rowKey, this.timestamp);
      }

      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();

      ConsoleTable.selectHead();
      int count = 0;

      while (scan.next(key, results) && checkLimit(count)) {
        Text rowKey = key.getRow();

        for (Text columnKey : results.keySet()) {
          String cellData = new String(results.get(columnKey), HConstants.UTF8_ENCODING);
          ConsoleTable.printLine(count, rowKey.toString(), columnKey.toString(), cellData);
        }
        count++;
      }
      ConsoleTable.selectFoot();
      scan.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Text[] getColumns(HBaseAdmin admin) {
    Text[] cols = null;

    try {
      if (this.columns.contains("*")) {
        HTableDescriptor[] tables = admin.listTables();
        if (this.tableName.equals(HConstants.ROOT_TABLE_NAME)
            || this.tableName.equals(HConstants.META_TABLE_NAME)) {
          cols = HConstants.COLUMN_FAMILY_ARRAY;
        } else {
          for (int i = 0; i < tables.length; i++) {
            if (tables[i].getName().equals(this.tableName)) {
              cols = tables[i].families().keySet().toArray(new Text[] {});
            }
          }
        }
      } else {
        List<Text> tmpList = new ArrayList<Text>();
        for (int i = 0; i < this.columns.size(); i++) {
          Text column = null;
          if(this.columns.get(i).contains(":"))
            column = new Text(this.columns.get(i));
          else
            column = new Text(this.columns.get(i) + ":");
          
          tmpList.add(column);
        }
        cols = tmpList.toArray(new Text[] {});
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return cols;
  }

  private boolean checkLimit(int count) {
    return (this.limit == 0)? true: (this.limit > count) ? true : false;
  }

  public void setTable(String table) {
    this.tableName = new Text(table);
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setWhere(boolean isWhereClause) {
    if (isWhereClause)
      this.whereClause = true;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = Long.parseLong(timestamp);
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  public void setRowKey(String rowKey) {
    if(rowKey == null) 
      this.rowKey = null; 
    else
      this.rowKey = new Text(rowKey);
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
