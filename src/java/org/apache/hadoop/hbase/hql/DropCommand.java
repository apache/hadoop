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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;

/**
 * Drops tables.
 */
public class DropCommand extends BasicCommand {
  private List<String> tableList;

  public DropCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(HBaseConfiguration conf) {
    if (tableList == null) {
      throw new IllegalArgumentException("List of tables is null.");
    }

    try {
      HBaseAdmin admin = new HBaseAdmin(conf);

      int count = 0;
      for (String table : tableList) {
        if (!admin.tableExists(new Text(table))) {
          println("'" + table + "' table not found.");
        } else {
          println("Dropping " + table + "... Please wait.");
          admin.deleteTable(new Text(table));
          count++;
        }
      }

      if (count > 0) {
        return new ReturnMsg(1, count + " table(s) dropped successfully.");
      } else {
        return new ReturnMsg(0, count + " table(s) dropped.");
      }
    } catch (IOException e) {
      return new ReturnMsg(0, extractErrMsg(e));
    }
  }

  public void setTableList(List<String> tableList) {
    this.tableList = tableList;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.DDL;
  }
}
