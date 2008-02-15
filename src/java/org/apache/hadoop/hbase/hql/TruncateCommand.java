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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Text;

/**
 * Truncate table is used to clean all data from a table.
 */
public class TruncateCommand extends BasicCommand {
  private Text tableName;

  public TruncateCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(final HBaseConfiguration conf) {
    if (this.tableName == null)
      return new ReturnMsg(0, "Syntax error : Please check 'Truncate' syntax.");

    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (!admin.tableExists(tableName)) {
        return new ReturnMsg(0, "Table not found.");
      }

      HTableDescriptor[] tables = admin.listTables();
      HColumnDescriptor[] columns = null;
      for (int i = 0; i < tables.length; i++) {
        if (tables[i].getName().equals(tableName)) {
          columns = tables[i].getFamilies().values().toArray(
              new HColumnDescriptor[] {});
          break;
        }
      }
      println("Truncating a '" + tableName + "' table ... Please wait.");

      admin.deleteTable(tableName); // delete the table
      HTableDescriptor tableDesc = new HTableDescriptor(tableName.toString());
      for (int i = 0; i < columns.length; i++) {
        tableDesc.addFamily(columns[i]);
      }
      admin.createTable(tableDesc); // re-create the table
    } catch (IOException e) {
      return new ReturnMsg(0, "error msg : " + e.toString());
    }
    return new ReturnMsg(0, "'" + tableName + "' is successfully truncated.");
  }

  public void setTableName(String tableName) {
    this.tableName = new Text(tableName);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.DDL;
  }
}
