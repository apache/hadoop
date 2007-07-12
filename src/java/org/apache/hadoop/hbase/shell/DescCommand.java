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

import org.apache.hadoop.hbase.HClient;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Text;

public class DescCommand extends BasicCommand {
  String argument;

  public ReturnMsg execute(HClient client) {
    if (this.argument == null) 
      return new ReturnMsg(0, "Syntax error : Please check 'Describe' syntax.");

    try {
      HTableDescriptor[] tables = client.listTables();
      Text[] columns = null;

      for (int i = 0; i < tables.length; i++) {
        if (tables[i].getName().toString().equals(this.argument)) {
          columns = tables[i].families().keySet().toArray(new Text[] {});
        }
      }

      if (columns == null) {
        return new ReturnMsg(0, "Table not found.");
      }

      ConsoleTable.printHead("ColumnFamily Name");
      for (int ii = 0; ii < columns.length; ii++) {
        String familyName = columns[ii].toString().replace(FAMILY_INDICATOR, "");
        ConsoleTable.printTable(ii, familyName);
      }
      ConsoleTable.printFoot();

      return new ReturnMsg(1, columns.length + " columnfamilie(s) found.");
    } catch (IOException e) {
      return new ReturnMsg(0, "error msg : " + e.toString());
    }
  }

  public void setArgument(String argument) {
    this.argument = argument;
  }
}
