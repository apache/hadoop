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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.io.Text;

/**
 * Drops tables.
 */
public class DropCommand extends BasicCommand {
  private List<String> tableList;

  public ReturnMsg execute(Configuration conf) {
    if (tableList == null) {
      throw new IllegalArgumentException("List of tables is null");
    }
 
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      
      for (String table : tableList) {
        System.out.println("Dropping " + table + "... Please wait.");
        admin.deleteTable(new Text(table));
      }
      
      return new ReturnMsg(1, "Table(s) dropped successfully.");
    } catch (IOException e) {
      return new ReturnMsg(0, extractErrMsg(e));
    }
  }

  public void setTableList(List<String> tableList) {
    this.tableList = tableList;
  }
}