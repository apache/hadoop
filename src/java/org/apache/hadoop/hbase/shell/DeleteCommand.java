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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HClient;
import org.apache.hadoop.io.Text;

public class DeleteCommand extends BasicCommand {
  String table;

  Map<String, List<String>> condition;

  public ReturnMsg execute(HClient client) {
    if (this.table == null || condition == null)
      return new ReturnMsg(0, "Syntax error : Please check 'Delete' syntax.");

    try {
      client.openTable(new Text(this.table));
      long lockId = client.startUpdate(getRow());

      if (getColumn() != null) {

        client.delete(lockId, getColumn());

      } else {
        Set<Text> keySet = client.getRow(getRow()).keySet();
        Text[] columnKey = keySet.toArray(new Text[keySet.size()]);

        for (int i = 0; i < columnKey.length; i++) {
          client.delete(lockId, columnKey[i]);
        }
      }

      client.commit(lockId);

      return new ReturnMsg(1, "1 deleted successfully. ");
    } catch (IOException e) {
      return new ReturnMsg(0, "error msg : " + e.toString());
    }
  }

  public void setTable(String table) {
    this.table = table;
  }

  public void setCondition(Map<String, List<String>> cond) {
    this.condition = cond;
  }

  public Text getRow() {
    return new Text(this.condition.get("row").get(1));
  }

  public Text getColumn() {
    if (this.condition.containsKey("column")) {
      return new Text(this.condition.get("column").get(1));
    } else {
      return null;
    }
  }
}
