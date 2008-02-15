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
import org.apache.hadoop.io.Text;

/**
 * Enables tables.
 */
public class EnableCommand extends BasicCommand {
  private String tableName;

  public EnableCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(HBaseConfiguration conf) {
    assert tableName != null;
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (!admin.tableExists(new Text(tableName))) {
        return new ReturnMsg(0, "'" + tableName + "'" + TABLE_NOT_FOUND);
      }
      admin.enableTable(new Text(tableName));
      return new ReturnMsg(1, "Table enabled successfully.");
    } catch (IOException e) {
      String[] msg = e.getMessage().split("[\n]");
      return new ReturnMsg(0, msg[0]);
    }
  }

  public void setTable(String table) {
    this.tableName = table;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.DDL;
  }
}
