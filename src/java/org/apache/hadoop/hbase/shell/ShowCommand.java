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
import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Shows all available tables.
 */
public class ShowCommand extends BasicCommand {
  private static final String [] HEADER = new String [] {"Table Name"};
  private String command;

  public ReturnMsg execute(Configuration conf) {
    if (this.command == null) {
      return new ReturnMsg(0, "Syntax error : Please check 'Show' syntax");
    }
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      int tableLength = 0;
      if ("tables".equals(this.command)) {
        HTableDescriptor[] tables = admin.listTables();
        tableLength = tables.length;
        if (tableLength == 0) {
          return new ReturnMsg(0, "Table not found");
        }
        TableFormatter formatter = TableFormatterFactory.get();
        formatter.header(HEADER);
        for (int i = 0; i < tableLength; i++) {
          String tableName = tables[i].getName().toString();
          formatter.row(new String [] {tableName});
        }
        formatter.footer();
        return new ReturnMsg(1, tableLength + " table(s) in set");
      }
      return new ReturnMsg(0, "Missing parameters. Please check 'Show' syntax");
    } catch (IOException e) {
      return new ReturnMsg(0, "error msg : " + e.toString());
    }
  }

  public void setArgument(String argument) {
    this.command = argument;
  }
}