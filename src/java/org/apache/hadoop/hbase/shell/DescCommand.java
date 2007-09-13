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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConnection;
import org.apache.hadoop.hbase.HConnectionManager;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Text;

/**
 * Prints information about tables.
 */
public class DescCommand extends BasicCommand {
  
  private Text tableName;

  public ReturnMsg execute(Configuration conf) {
    if (this.tableName == null) 
      return new ReturnMsg(0, "Syntax error : Please check 'Describe' syntax.");
    try {
      HConnection conn = HConnectionManager.getConnection(conf);
      if (!conn.tableExists(this.tableName)) {
        return new ReturnMsg(0, "Table not found.");
      }
      HTableDescriptor [] tables = conn.listTables();
      HColumnDescriptor [] columns = null;
      for (int i = 0; i < tables.length; i++) {
        if (tables[i].getName().equals(this.tableName)) {
          columns = tables[i].getFamilies().values().
            toArray(new HColumnDescriptor [] {});
          break;
        }
      }
      ConsoleTable.printHead("ColumnFamily");
      for (int ii = 0; ii < columns.length; ii++) {
        String tmp = columns[ii].toString();
        ConsoleTable.printTable(ii, tmp.substring(1, tmp.length() - 1));
      }
      ConsoleTable.printFoot();
      return new ReturnMsg(1, columns.length + " columnfamilie(s) found.");
    } catch (IOException e) {
      return new ReturnMsg(0, "error msg : " + e.toString());
    }
  }

  public void setArgument(String table) {
    this.tableName = new Text(table);
  } 
}